/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.kafka.scaladsl

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.function.LongBinaryOperator

import akka.Done
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.{KillSwitches, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.{Inside, OptionValues}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class PartitionedSourcesSpec extends SpecBase with TestcontainersKafkaLike with Inside with OptionValues {

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
        .runWith(TestSink())

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
        .runWith(TestSink())

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
      val initialized = Promise[Unit]()

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
      val initialized = Promise[Unit]()

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

      val (control1, firstConsumer) = source.toMat(TestSink())(Keep.both).run()

      eventually {
        assert(partitionsAssigned, "first consumer should get asked for offsets")
      }

      val secondConsumer = source.runWith(TestSink())

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
        .run()

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
        .run()

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

      val maxEncounteredOffsets = (0 until partitions).map(_ -> new AtomicLong()).toMap

      def externalCommitOffset(partitionId: Int, offset: Long): Future[Unit] = {
        log.info(s"Commit $partitionId at $offset")
        maxEncounteredOffsets(partitionId).accumulateAndGet(offset, new LongBinaryOperator {
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
            topicPartitions => {
              Future.successful(topicPartitions.map(tp => tp -> maxEncounteredOffsets(tp.partition()).get()).toMap)
            }
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
