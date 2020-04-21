/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.LongBinaryOperator

import akka.Done
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.kafka.tests.AlpakkaAssignor
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{KillSwitches, OverflowStrategy}
import akka.testkit.TestProbe
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class PartitionedSourcesSpec extends SpecBase with TestcontainersKafkaLike with Inside with OptionValues with Repeated {

  override def sleepAfterProduce: FiniteDuration = 500.millis

  "Partitioned source" must {

    "begin consuming from the beginning of the topic" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()

      awaitProduce(produce(topic, 1 to 100))

      val probe = Consumer
        .plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group),
                                            Subscriptions.topics(topic),
                                            _ => Future.successful(Map.empty))
        .flatMapMerge(1, _._2)
        .map(_.value())
        .runWith(TestSink.probe)

      probe
        .request(100)
        .expectNextN((1 to 100).map(_.toString))

      probe.cancel()
    }

    "begin consuming from the middle of the topic" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()

      awaitProduce(produce(topic, 1 to 100))

      val probe = Consumer
        .plainPartitionedManualOffsetSource(consumerDefaults.withGroupId(group),
                                            Subscriptions.topics(topic),
                                            tp => Future.successful(tp.map(_ -> 51L).toMap))
        .flatMapMerge(1, _._2)
        .map(_.value())
        .runWith(TestSink.probe)

      probe
        .request(50)
        .expectNextN((52 to 100).map(_.toString))

      probe.cancel()
    }

    "get a source per partition" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 400L

      val topic = createTopic(1, partitions)
      val allTps = (0 until partitions).map(p => new TopicPartition(topic, p))
      val group = createGroupId()
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withGroupId(group)

      var createdSubSources = List.empty[TopicPartition]

      val control = Consumer
        .plainPartitionedSource(sourceSettings, Subscriptions.topics(topic))
        .groupBy(partitions, _._1)
        .mapAsync(8) {
          case (tp, source) =>
            log.info(s"Sub-source for $tp started")
            createdSubSources = tp :: createdSubSources
            source
              .scan(0L)((c, _) => c + 1)
              .runWith(Sink.last)
              .map { res =>
                log.info(s"Sub-source for $tp completed: Received [$res] messages in total.")
                res
              }
        }
        .mergeSubstreams
        .scan(0L)((c, subValue) => c + subValue)
        .toMat(Sink.last)(DrainingControl.apply)
        .run()

      // waits until all partitions are assigned to the single consumer
      waitUntilConsumerSummary(group) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == partitions
      }

      val producer = Source(1L to totalMessages)
        .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

      producer.futureValue shouldBe Done
      sleep(2.seconds)
      val streamMessages = control.drainAndShutdown().futureValue
      createdSubSources should contain allElementsOf allTps
      streamMessages shouldBe totalMessages
    }

    "rebalance when a new consumer comes up" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 400L

      val initialMessage = 0L
      val initialized = Promise[Unit]

      val topic = createTopic(1, partitions)
      val group = createGroupId()
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withGroupId(group)

      val receivedMessages = new AtomicLong(0)

      def createAndRunConsumer() =
        Consumer
          .plainPartitionedSource(sourceSettings, Subscriptions.topics(topic))
          .groupBy(partitions, _._1)
          .mapAsync(8) {
            case (tp, source) =>
              log.info(s"Sub-source for $tp")
              source
                .map { v =>
                  initialized.trySuccess(())
                  v
                }
                .filter(_.value() != initialMessage.toString)
                .map { v =>
                  receivedMessages.incrementAndGet()
                  v
                }
                .scan(0L)((c, _) => c + 1)
                .runWith(Sink.last)
                .map { res =>
                  log.info(s"Sub-source for $tp completed: Received [$res] messages in total.")
                  res
                }
          }
          .mergeSubstreams
          .scan(0L)((c, subValue) => c + subValue)
          .toMat(Sink.last)(DrainingControl.apply)
          .run()

      val control = createAndRunConsumer()

      // waits until all partitions are assigned to the single consumer
      waitUntilConsumerSummary(group) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == partitions
      }

      var control2: DrainingControl[Long] = null

      val producer = Source
        .unfold(()) { _ =>
          if (!initialized.future.isCompleted) {
            Some(((), initialMessage))
          } else {
            None
          }
        }
        .concat(Source(1L to totalMessages))
        .map { number =>
          if (number == totalMessages / 2) {
            // create another consumer with the same groupId to trigger re-balancing
            control2 = createAndRunConsumer()
            sleep(4.seconds, "to let the new consumer start")
          }
          number
        }
        .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

      producer.futureValue shouldBe Done

      // waits until partitions are assigned across both consumers
      waitUntilConsumerSummary(group) {
        case consumer1 :: consumer2 :: Nil =>
          val half = partitions / 2
          consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
      }

      control2 should not be null
      eventually {
        receivedMessages.get() should be >= totalMessages
      }
      val stream1messages = control.drainAndShutdown().futureValue
      val stream2messages = control2.drainAndShutdown().futureValue
      stream1messages + stream2messages shouldBe totalMessages
      stream2messages should be >= (totalMessages / 4)
    }

    // See even test in IntegrationSpec with the same name.
    "signal rebalance events to actor" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 400L
      val receivedMessages = new AtomicLong(0)
      val initialMessage = 0L
      val initialized = Promise[Unit]

      val topic = createTopic(1, partitions)
      val allTps = (0 until partitions).map(p => new TopicPartition(topic, p))
      val group = createGroupId()
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withGroupId(group)

      val rebalanceActor = TestProbe()

      val topicSubscription = Subscriptions.topics(topic)
      val subscription1 = topicSubscription.withRebalanceListener(rebalanceActor.ref)

      def createAndRunConsumer(subscription: AutoSubscription) =
        Consumer
          .plainPartitionedSource(sourceSettings, subscription)
          .flatMapMerge(partitions, _._2)
          .map { v =>
            initialized.trySuccess(())
            v
          }
          .filter(_.value() != initialMessage.toString)
          .map { v =>
            receivedMessages.incrementAndGet()
            v
          }
          .scan(0L)((c, _) => c + 1)
          .toMat(Sink.last)(DrainingControl.apply)
          .run()

      val control = createAndRunConsumer(subscription1)

      // waits until all partitions are assigned to the single consumer
      waitUntilConsumerSummary(group) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == partitions
      }

      var control2: DrainingControl[Long] = null

      val producer = Source
        .unfold(()) { _ =>
          if (!initialized.future.isCompleted) {
            Some(((), initialMessage))
          } else {
            None
          }
        }
        .concat(Source(1L to totalMessages))
        .map { number =>
          if (number == totalMessages / 2) {
            // create another consumer with the same groupId to trigger re-balancing
            control2 = createAndRunConsumer(topicSubscription)
            sleep(4.seconds, "to let the new consumer start")
          }
          number
        }
        .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

      producer.futureValue shouldBe Done

      rebalanceActor.expectMsg(TopicPartitionsAssigned(subscription1, Set(allTps: _*)))

      // waits until partitions are assigned across both consumers
      waitUntilConsumerSummary(group) {
        case consumer1 :: consumer2 :: Nil =>
          val half = partitions / 2
          consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
      }

      control2 should not be null
      eventually {
        receivedMessages.get() should be >= totalMessages
      }

      rebalanceActor.expectMsg(TopicPartitionsRevoked(subscription1, Set(allTps: _*)))
      rebalanceActor.expectMsg(TopicPartitionsAssigned(subscription1, Set(allTps(0), allTps(1))))

      val stream1messages = control.drainAndShutdown().futureValue
      val stream2messages = control2.drainAndShutdown().futureValue
      stream1messages + stream2messages shouldBe totalMessages
      stream2messages should be >= (totalMessages / 4)
    }

    "call the onRevoked hook" in assertAllStagesStopped {
      val partitions = 4
      val topic = createTopic(1, partitions)
      val group = createGroupId()

      var partitionsAssigned = false
      var revoked: Option[Set[TopicPartition]] = None

      val source = Consumer
        .plainPartitionedManualOffsetSource(
          consumerDefaults.withGroupId(group),
          Subscriptions.topics(topic),
          getOffsetsOnAssign = _ => {
            partitionsAssigned = true
            Future.successful(Map.empty)
          },
          onRevoke = tps => revoked = Some(tps)
        )
        .flatMapMerge(1, _._2)
        .map(_.value())

      val (control1, firstConsumer) = source.toMat(TestSink.probe)(Keep.both).run()

      eventually {
        assert(partitionsAssigned, "first consumer should get asked for offsets")
      }

      val secondConsumer = source.runWith(TestSink.probe)

      eventually {
        revoked.value should have size partitions / 2L
      }

      firstConsumer.cancel()
      secondConsumer.cancel()
      control1.isShutdown.futureValue should be(Done)
    }

    "not leave gaps when subsource is cancelled" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()
      val totalMessages = 100

      awaitProduce(produce(topic, 1 to totalMessages))

      val consumedMessages =
        Consumer
          .plainPartitionedSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic))
          .log(topic)
          .flatMapMerge(
            1, {
              case (tp, source) =>
                source
                  .log(tp.toString, _.offset())
                  .take(10)
            }
          )
          .map(_.value().toInt)
          .takeWhile(_ < totalMessages, inclusive = true)
          .scan(0)((c, _) => c + 1)
          .runWith(Sink.last)

      consumedMessages.futureValue shouldBe totalMessages
    }

    "not leave gaps when subsource fails" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()
      val totalMessages = 105

      awaitProduce(produce(topic, 1 to totalMessages))

      val (queue, accumulator) = Source
        .queue[Long](8, OverflowStrategy.backpressure)
        .toMat(Sink.fold(0)((c, _) => c + 1))(Keep.both)
        .run

      val (killSwitch, consumerCompletion) = Consumer
        .plainPartitionedSource(consumerDefaults.withGroupId(group), Subscriptions.topics(topic))
        .log(topic)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.foreach {
          case (tp, source) =>
            source
              .log(tp.toString, _.offset())
              .mapAsync(parallelism = 1)(rec => queue.offer(rec.offset()).map(_ => rec))
              .map(_.value().toInt)
              .takeWhile(_ < totalMessages, inclusive = true)
              .map { value =>
                if (value % 10 == 0) throw new Error("Stopping subsource")
                value
              }
              .runWith(Sink.onComplete {
                case Success(_) => queue.complete()
                case _ =>
              })
        })(Keep.both)
        .run

      accumulator.futureValue shouldBe totalMessages
      killSwitch.shutdown()
      consumerCompletion.futureValue
    }

    "not leave gaps after a rebalance with manual offsets" in assertAllStagesStopped {
      val partitions = 2
      val messagesPerPartition = 100L
      val totalMessages = messagesPerPartition * partitions

      val topic = createTopic(1, partitions)
      val group = createGroupId()
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withGroupId(group)

      val partitionsOffsetCommitMap: Map[Int, AtomicLong] = (0 until partitions).map(_ -> new AtomicLong()).toMap

      def getOffsetsOnAssign(topicPartitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
        Future.successful {
          val offsets = topicPartitions.map(tp => tp -> partitionsOffsetCommitMap(tp.partition()).get()).toMap
          log.debug(s"getOffsetsOnAssign for topicPartitions $topicPartitions, offsets returned: $offsets")
          offsets
        }

      def externalCommitOffset(partitionId: Int, offset: Long): Future[Unit] = {
        log.info(s"Commit $partitionId at $offset")
        partitionsOffsetCommitMap(partitionId).accumulateAndGet(offset, new LongBinaryOperator {
          override def applyAsLong(left: Long, right: Long): Long = left.max(right)
        })
        Future.successful(())
      }

      val receivedMessages = new AtomicLong(0)

      val producer = Source(1L to totalMessages)
        .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
        .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

      producer.futureValue shouldBe Done

      def createAndRunConsumer(
          recordProcessor: ConsumerRecord[String, String] => Future[ConsumerRecord[String, String]]
      ) =
        Consumer
          .plainPartitionedManualOffsetSource(
            sourceSettings,
            Subscriptions.topics(topic),
            getOffsetsOnAssign
          )
          .groupBy(partitions, _._1)
          .mapAsync(8) {
            case (tp, source) =>
              log.info(s"Sub-source for $tp")
              source
                .mapAsync(1)(recordProcessor)
                .runWith(Sink.ignore)
                .map { res =>
                  log.info(s"Sub-source for $tp completed")
                  res
                }
          }
          .mergeSubstreams
          .toMat(Sink.ignore)(DrainingControl.apply)
          .run()

      val firstHalfLatch = new CountDownLatch(1)
      val latch = new CountDownLatch(1)

      val control1 = createAndRunConsumer(r => {
        log.debug(s"control1 got ${r.partition()} at offset ${r.offset()}")
        val f = if (r.offset() <= messagesPerPartition / 2) {
          Future.successful(r)
        } else {
          firstHalfLatch.countDown()
          Future {
            latch.await(20, TimeUnit.SECONDS)
            r
          }
        }
        f.andThen {
          case Success(_) =>
            receivedMessages.incrementAndGet()
            externalCommitOffset(r.partition(), r.offset())
        }
      })

      firstHalfLatch.await(30, TimeUnit.SECONDS) should be(true)
      log.debug("First half latch counted down")

      val control2 = createAndRunConsumer(r => {
        log.debug(s"control2 got ${r.partition()} at offset ${r.offset()}")
        Future
          .successful {
            latch.await(30, TimeUnit.SECONDS)
            r
          }
          .andThen {
            case Success(_) =>
              receivedMessages.incrementAndGet()
              externalCommitOffset(r.partition(), r.offset())
          }
      })

      // waits until partitions are assigned across both consumers
      waitUntilConsumerSummary(group) {
        case consumer1 :: consumer2 :: Nil =>
          val half = partitions / 2
          consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
      }

      log.debug("Now there are 2 consumers, going to continue")
      latch.countDown()

      eventually {
        receivedMessages.get() should be >= totalMessages
      }
      control1.drainAndShutdown().futureValue should be(Done)
      control2.drainAndShutdown().futureValue should be(Done)
    }

    // bugfix for https://github.com/akka/alpakka-kafka/issues/1086
    "seek back to last externally committed offset when partition assigned back to same consumer group member" in assertAllStagesStopped {
      case class CommitInfo(offset: Long, partition: Int, externalCommit: Boolean, kafkaCommit: Boolean)
      val partitions = 2
      val messagesPerPartition = 100
      val topic = createTopic(1, partitions)
      val group = createGroupId()
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)
      val consumerClientId1 = "consumer-1"
      val consumerClientId2 = "consumer-2"
      val runConsumer2 = Promise[Done]()

      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[AlpakkaAssignor].getName)
        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withGroupId(group)

      val partitionsOffsetCommitMap: Map[Int, AtomicLong] = (0 until partitions).map(_ -> new AtomicLong()).toMap

      def getOffsetsOnAssign(topicPartitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
        Future.successful {
          val offsets = topicPartitions.map { tp =>
            tp -> partitionsOffsetCommitMap(tp.partition()).get()
          }.toMap
          log.debug(s"getOffsetsOnAssign for topicPartitions $topicPartitions, offsets returned: $offsets")
          offsets
        }

      def externalCommit(partitionId: Int, offset: Long): Future[Unit] = {
        partitionsOffsetCommitMap(partitionId).accumulateAndGet(offset, new LongBinaryOperator {
          override def applyAsLong(left: Long, right: Long): Long = left.max(right)
        })
        log.info(s"External commit complete. Partition: $partitionId, Offset: $offset")
        Future.successful(())
      }

      def kafkaCommit(msg: CommittableMessage[_, _]): Future[Unit] =
        msg.committableOffset
          .commitInternal()
          .map(_ => ())
          .andThen {
            case Success(_) =>
              log.info(s"Kafka commit complete. Partition: {}, Offset: {}", msg.record.partition(), msg.record.offset())
          }

      produce(topic, range = 1 to messagesPerPartition, partition = tp0.partition())

      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(tp0, tp1)
        )
      )

      def createAndRunConsumer(
          consumerClientId: String,
          recordProcessor: CommittableMessage[String, String] => Future[CommitInfo] = { msg =>
            Future.successful(
              CommitInfo(msg.record.offset(), msg.record.partition(), externalCommit = false, kafkaCommit = false)
            )
          }
      ): (Consumer.Control, TestSubscriber.Probe[(TopicPartition, TestSubscriber.Probe[CommitInfo])]) =
        Consumer
          .committablePartitionedManualOffsetSource(
            sourceSettings.withClientId(consumerClientId),
            Subscriptions.topics(topic),
            getOffsetsOnAssign
          )
          .groupBy(partitions, _._1)
          .map {
            case (tp, source) =>
              log.info("Sub-source for {} emitted", tp)
              val probe = source
                .mapAsync(1)(recordProcessor)
                .runWith(TestSink.probe)
              (tp, probe)
          }
          .mergeSubstreams
          .toMat(TestSink.probe)(Keep.both)
          .run()

      log.debug("Running {}", consumerClientId1)
      val (control1, probe1) = createAndRunConsumer(
        consumerClientId1,
        msg => {
          val r = msg.record
          log.debug(s"$consumerClientId1 got ${r.partition()} at offset ${r.offset()}")

          // commit kafka and externally when offset <= messagesPerPartition / 2 (50)
          if (r.offset() <= messagesPerPartition / 2)
            for (_ <- kafkaCommit(msg); _ <- externalCommit(r.partition(), r.offset()))
              yield CommitInfo(r.offset(), r.partition(), externalCommit = true, kafkaCommit = true)
          // only commit to Kafka and signal consumer-2 to join
          else {
            for (_ <- kafkaCommit(msg))
              yield {
                runConsumer2.trySuccess(Done) // let consumer-2 join the group
                CommitInfo(r.offset(), r.partition(), externalCommit = false, kafkaCommit = true)
              }
          }
        }
      )

      // request 2 sub sources (tp0, tp1) from consumer-1
      // sub sources are emitted out of order, so request all of them and pick out partition 0
      probe1.request(2)
      val probe1SubSources = probe1.expectNextN(2)
      val subSourceTp0Probe = probe1SubSources
        .find {
          case (tp, _) if tp == tp0 => true
          case _ => false
        }
        .map { case (_, probe) => probe }
        .get

      // request 52 msgs from tp0 sub source
      subSourceTp0Probe.request(52)
      // messages 0 to 50 will be externally committed
      subSourceTp0Probe.expectNextN(51) foreach { commit =>
        commit.externalCommit shouldBe true
      }
      // message 52 (offset = 51) will only commit to Kafka, maintaining the last external commit at offset 50
      subSourceTp0Probe.expectNext() shouldEqual CommitInfo(
        offset = 51,
        partition = 0,
        externalCommit = false,
        kafkaCommit = true
      )

      runConsumer2.future.futureValue

      // rebalance partitions, but keep tp0 on consumer-1 group member
      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(tp0),
          consumerClientId2 -> Set(tp1)
        )
      )

      log.debug("Running {}", consumerClientId2)
      val (control2, probe2) = createAndRunConsumer(consumerClientId2)
      probe2.request(1)
      val probe2SubSources = probe2.expectNextN(1)

      // waits until partitions are assigned across both consumers
      waitUntilConsumerSummary(group) {
        case consumer1 :: consumer2 :: Nil =>
          val half = partitions / 2
          consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
      }

      // wait for tp0 sub source to eventually re-seek back to last externally committed offset 50, but there might
      // still be some messages to drain first that were in-flight before and during rebalance
      eventually {
        subSourceTp0Probe.request(1)
        subSourceTp0Probe.expectNext() shouldEqual CommitInfo(offset = 50,
                                                              partition = 0,
                                                              externalCommit = true,
                                                              kafkaCommit = true)
      }

      probe1.cancel()
      probe2.cancel()
      probe1SubSources.foreach(_._2.cancel())
      probe2SubSources.foreach(_._2.cancel())
      control1.isShutdown.futureValue should be(Done)
      control2.isShutdown.futureValue should be(Done)
    }

    "handle exceptions in stream without commit failures" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 100L
      val exceptionTriggered = new AtomicBoolean(false)

      val topic = createTopic(1, partitions)
      val allTps = (0 until partitions).map(p => new TopicPartition(topic, p))
      val group = createGroupId()
      val sourceSettings = consumerDefaults
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withGroupId(group)
        .withStopTimeout(2.seconds)

      var createdSubSources = List.empty[TopicPartition]

      var commitFailures = List.empty[(TopicPartition, Throwable)]

      val control = Consumer
        .committablePartitionedSource(sourceSettings, Subscriptions.topics(topic))
        .groupBy(partitions, _._1)
        .mapAsync(8) {
          case (tp, source) =>
            createdSubSources = tp :: createdSubSources
            source
              .log(s"subsource $tp", _.record.value())
              .mapAsync(4) { m =>
                // fail on first partition; otherwise delay slightly and emit
                if (tp.partition() == 0) {
                  log.debug(s"failing $tp source")
                  exceptionTriggered.set(true)
                  Future.failed(new RuntimeException("FAIL"))
                } else {
                  akka.pattern.after(50.millis, system.scheduler)(Future.successful(m))
                }
              }
              .log(s"subsource $tp pre commit")
              .mapAsync(1)(_.committableOffset.commitInternal().andThen {
                case Failure(e) =>
                  log.error("commit failure", e)
                  commitFailures ::= tp -> e
              })
              .scan(0L)((c, _) => c + 1)
              .runWith(Sink.last)
              .map { res =>
                log.info(s"Sub-source for $tp completed: Received [$res] messages in total.")
                res
              }
        }
        .mergeSubstreams
        .scan(0L)((c, n) => c + n)
        .toMat(Sink.last)(DrainingControl.apply)
        .run()

      // waits until all partitions are assigned to the single consumer
      waitUntilConsumerSummary(group) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == partitions
      }

      awaitProduce(
        Source(1L to totalMessages)
          .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
          .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))
      )
      eventually {
        exceptionTriggered.get() shouldBe true
      }

      val exception = control.drainAndShutdown().failed.futureValue
      createdSubSources should contain allElementsOf allTps
      exception.getMessage shouldBe "FAIL"

      // commits will fail if we shut down the consumer too early
      commitFailures shouldBe empty
    }
  }
}
