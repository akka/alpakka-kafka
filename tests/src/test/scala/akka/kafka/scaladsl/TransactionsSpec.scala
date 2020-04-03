/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.kafka.ConsumerMessage.PartitionOffset
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.kafka.{ProducerMessage, _}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.RecoverMethods._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TransactionsSpec extends SpecBase with TestcontainersKafkaLike with TransactionsOps {

  "A consume-transform-produce cycle" must {

    "complete in happy-path scenario" in {
      assertAllStagesStopped {
        val sourceTopic = createTopic(1)
        val sinkTopic = createTopic(2)
        val group = createGroupId(1)

        Await.result(produce(sourceTopic, 1 to 100), remainingOrDefault)

        val consumerSettings = consumerDefaults.withGroupId(group)

        val control =
          transactionalCopyStream(consumerSettings, txProducerDefaults, sourceTopic, sinkTopic, group, 10.seconds)
            .toMat(Sink.ignore)(Keep.left)
            .run()

        val probeConsumerGroup = createGroupId(2)

        val probeConsumer = valuesProbeConsumer(probeConsumerSettings(probeConsumerGroup), sinkTopic)

        probeConsumer
          .request(100)
          .expectNextN((1 to 100).map(_.toString))

        probeConsumer.cancel()
        Await.result(control.shutdown(), remainingOrDefault)
      }
    }

    "complete when messages are filtered out" in assertAllStagesStopped {
      val sourceTopic = createTopic(1)
      val sinkTopic = createTopic(2)
      val group = createGroupId(1)

      Await.result(produce(sourceTopic, 1 to 100), remainingOrDefault)

      val consumerSettings = consumerDefaults.withGroupId(group)

      val control = Transactional
        .source(consumerSettings, Subscriptions.topics(sourceTopic))
        .map { msg =>
          if (msg.record.value.toInt % 10 == 0) {
            ProducerMessage.passThrough[String, String, ConsumerMessage.PartitionOffset](msg.partitionOffset)
          } else {
            ProducerMessage.single(new ProducerRecord(sinkTopic, msg.record.key, msg.record.value), msg.partitionOffset)
          }
        }
        .via(Transactional.flow(producerDefaults, group))
        .toMat(Sink.ignore)(Keep.left)
        .run()

      val probeConsumerGroup = createGroupId(2)

      val probeConsumer = valuesProbeConsumer(probeConsumerSettings(probeConsumerGroup), sinkTopic)

      probeConsumer
        .request(100)
        .expectNextN((1 to 100).filterNot(_ % 10 == 0).map(_.toString))

      probeConsumer.cancel()
      Await.result(control.shutdown(), remainingOrDefault)
    }

    "complete with transient failure causing an abort with restartable source" in {
      assertAllStagesStopped {
        val sourceTopic = createTopic(1)
        val sinkTopic = createTopic(2)
        val group = createGroupId(1)

        Await.result(produce(sourceTopic, 1 to 1000), remainingOrDefault)

        val consumerSettings = consumerDefaults.withGroupId(group)

        var restartCount = 0
        var innerControl = null.asInstanceOf[Control]

        val restartSource = RestartSource.onFailuresWithBackoff(
          minBackoff = 0.1.seconds,
          maxBackoff = 1.seconds,
          randomFactor = 0.2
        ) { () =>
          restartCount += 1
          Transactional
            .source(consumerSettings, Subscriptions.topics(sourceTopic))
            .map { msg =>
              if (msg.record.value().toInt == 500 && restartCount < 2) {
                // add a delay that equals or exceeds EoS commit interval to trigger a commit for everything
                // up until this record (0 -> 500)
                Thread.sleep(producerDefaults.eosCommitInterval.toMillis + 10)
              }
              if (msg.record.value().toInt == 501 && restartCount < 2) {
                throw new RuntimeException("Uh oh.. intentional exception")
              } else {
                ProducerMessage.single(new ProducerRecord(sinkTopic, msg.record.key, msg.record.value),
                                       msg.partitionOffset)
              }
            }
            // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
            .mapMaterializedValue(innerControl = _)
            .via(Transactional.flow(producerDefaults, group))
        }

        restartSource.runWith(Sink.ignore)

        val probeGroup = createGroupId(2)

        val probeConsumer = valuesProbeConsumer(probeConsumerSettings(probeGroup), sinkTopic)

        probeConsumer
          .request(1000)
          .expectNextN((1 to 1000).map(_.toString))

        probeConsumer.cancel()
        Await.result(innerControl.shutdown(), remainingOrDefault)
      }
    }

    "complete with messages filtered out and transient failure causing an abort with restartable source" in assertAllStagesStopped {
      val sourceTopic = createTopic(1)
      val sinkTopic = createTopic(2)
      val group = createGroupId(1)

      Await.result(produce(sourceTopic, 1 to 100), remainingOrDefault)

      val consumerSettings = consumerDefaults.withGroupId(group)

      var restartCount = 0
      var innerControl = null.asInstanceOf[Control]

      val restartSource = RestartSource.onFailuresWithBackoff(
        minBackoff = 0.1.seconds,
        maxBackoff = 1.seconds,
        randomFactor = 0.2
      ) { () =>
        restartCount += 1
        Transactional
          .source(consumerSettings, Subscriptions.topics(sourceTopic))
          .map { msg =>
            if (msg.record.value().toInt == 50 && restartCount < 2) {
              // add a delay that equals or exceeds EoS commit interval to trigger a commit for everything
              // up until this record (0 -> 500)
              Thread.sleep(producerDefaults.eosCommitInterval.toMillis + 10)
            }
            if (msg.record.value().toInt == 51 && restartCount < 2) {
              throw new RuntimeException("Uh oh..")
            } else {
              ProducerMessage.Message(new ProducerRecord(sinkTopic, msg.record.key, msg.record.value),
                                      msg.partitionOffset)
            }
          }
          .map { msg =>
            if (msg.record.value.toInt % 10 == 0) {
              ProducerMessage.passThrough[String, String, PartitionOffset](msg.passThrough)
            } else msg
          }
          // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
          .mapMaterializedValue(innerControl = _)
          .via(Transactional.flow(producerDefaults, group))
      }

      restartSource.runWith(Sink.ignore)

      val probeGroup = createGroupId(2)

      val probeConsumer = valuesProbeConsumer(probeConsumerSettings(probeGroup), sinkTopic)

      probeConsumer
        .request(100)
        .expectNextN((1 to 100).filterNot(_ % 10 == 0).map(_.toString))

      probeConsumer.cancel()
      Await.result(innerControl.shutdown(), remainingOrDefault)
    }

    "provide consistency when using multiple transactional streams" in assertAllStagesStopped {
      val sourceTopic = createTopic(1)
      val sinkTopic = createTopic(2, partitions = 4)
      val group = createGroupId(1)

      val elements = 50
      val batchSize = 10
      Await.result(produce(sourceTopic, 1 to elements), remainingOrDefault)

      val consumerSettings = consumerDefaults.withGroupId(group)

      def runStream(id: String): Consumer.Control = {
        val control: Control =
          transactionalCopyStream(consumerSettings,
                                  txProducerDefaults,
                                  sourceTopic,
                                  sinkTopic,
                                  s"$group-$id",
                                  10.seconds)
            .toMat(Sink.ignore)(Keep.left)
            .run()
        control
      }

      val controls: Seq[Control] = (0 until elements / batchSize)
        .map(_.toString)
        .map(runStream)

      val probeConsumerGroup = createGroupId(2)

      val probeConsumer = valuesProbeConsumer(probeConsumerSettings(probeConsumerGroup), sinkTopic)

      probeConsumer
        .request(elements.toLong)
        .expectNextUnorderedN((1 to elements).map(_.toString))

      probeConsumer.cancel()

      val futures: Seq[Future[Done]] = controls.map(_.shutdown())
      Await.result(Future.sequence(futures), remainingOrDefault)
    }

    "support copy stream with merging and multi messages" in assertAllStagesStopped {
      val sourceTopic = createTopic(1)
      val sumsTopic = createTopic(2)
      val concatsTopic = createTopic(3)
      val group = createGroupId(1)

      Await.result(produce(sourceTopic, 1 to 100), remainingOrDefault)

      val consumerSettings = consumerDefaults.withGroupId(group)

      val control = {
        Transactional
          .source(consumerSettings, Subscriptions.topics(sourceTopic))
          .groupedWithin(10, 5.seconds)
          .map { msgs =>
            val values = msgs.map(_.record.value().toInt)
            log.debug(s"msg group length {}. values: {}", values.length, values)

            val sum = msgs.map(_.record.value().toInt).sum.toString
            val concat = msgs.map(_.record.value()).reduce(_ + _)

            ProducerMessage.multi(
              immutable.Seq(
                new ProducerRecord[String, String](sumsTopic, sum),
                new ProducerRecord[String, String](concatsTopic, concat)
              ),
              msgs.map(_.partitionOffset).maxBy(_.offset)
            )
          }
          .via(Transactional.flow(producerDefaults, group))
          .toMat(Sink.ignore)(Keep.left)
          .run()
      }

      val sumsConsumer = valuesProbeConsumer(probeConsumerSettings(createGroupId(2)), sumsTopic)

      sumsConsumer
        .request(10)
        .expectNextN((1 to 100).grouped(10).map(_.sum.toString).toList)

      val concatsConsumer = valuesProbeConsumer(probeConsumerSettings(createGroupId(2)), concatsTopic)

      concatsConsumer
        .request(10)
        .expectNextN((1 to 100).map(_.toString).grouped(10).map(_.reduce(_ + _)).toList)

      sumsConsumer.cancel()
      concatsConsumer.cancel()
      Await.result(control.shutdown(), remainingOrDefault)
    }

    "complete partitioned source in happy-path scenario" in {
      assertAllStagesStopped {
        val elements = 200
        val maxPartitions = 2
        val sourceTopic = createTopic(1, maxPartitions)
        val sinkTopic = createTopic(2, maxPartitions)
        val group = createGroupId(1)
        val transactionalId = createTransactionalId()

        val consumerSettings = consumerDefaults.withGroupId(group)

        def runTransactional =
          Transactional
            .partitionedSource(consumerSettings, Subscriptions.topics(sourceTopic))
            .mapAsyncUnordered(maxPartitions) {
              case (_, source) =>
                source
                  .map { msg =>
                    ProducerMessage.single(new ProducerRecord[String, String](sinkTopic,
                                                                              msg.record.partition(),
                                                                              msg.record.key(),
                                                                              msg.record.value),
                                           msg.partitionOffset)
                  }
                  .runWith(Transactional.sink(producerDefaults, transactionalId))
            }
            .toMat(Sink.ignore)(Keep.left)
            .run()

        val control = runTransactional
        val control2 = runTransactional

        waitUntilConsumerSummary(group) {
          case consumer1 :: consumer2 :: Nil =>
            val half = maxPartitions / 2
            consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
        }

        val testProducerSettings = producerDefaults.withProducer(testProducer)
        Await.result(
          produceToAllPartitions(testProducerSettings, sourceTopic, maxPartitions, 1 to elements),
          remainingOrDefault
        )

        val consumer = consumePartitionOffsetValues(
          probeConsumerSettings(createGroupId(2)),
          sinkTopic,
          elementsToTake = (elements * maxPartitions).toLong
        )

        val actualValues: immutable.Seq[(Int, Long, String)] = Await.result(consumer, 60.seconds)
        assertPartitionedConsistency(elements, maxPartitions, actualValues)

        Await.result(control.shutdown(), remainingOrDefault)
        Await.result(control2.shutdown(), remainingOrDefault)
      }
    }

    "complete partitioned source with a sub source failure and rebalance of partitions to second instance" in {
      assertAllStagesStopped {
        val elements = 200
        val maxPartitions = 2
        val sourceTopic = createTopic(1, maxPartitions)
        val sinkTopic = createTopic(2, maxPartitions)
        val group = createGroupId(1)
        val transactionalId = createTransactionalId()

        val testProducerSettings = producerDefaults.withProducer(testProducer)
        val consumerSettings = consumerDefaults
          .withGroupId(group)
          .withStopTimeout(0.millis) // time to wait to schedule Stop to be sent to consumer actor
          .withCloseTimeout(0.millis) // timeout while waiting for KafkaConsumer.close

        val failureOccurred = new AtomicBoolean()

        def runTransactional(): (Control, Future[Done]) =
          Transactional
            .partitionedSource(consumerSettings, Subscriptions.topics(sourceTopic))
            .mapAsyncUnordered(maxPartitions) {
              case (tp, source) =>
                source
                  .map { msg =>
                    if (tp.partition() == 1 && msg.record.value.toInt == elements / 2 && !failureOccurred.get()) {
                      failureOccurred.set(true)
                      throw new Exception("sub source failure")
                    }
                    ProducerMessage.single(new ProducerRecord[String, String](sinkTopic,
                                                                              msg.record.partition(),
                                                                              msg.record.key(),
                                                                              msg.record.value),
                                           msg.partitionOffset)
                  }
                  .runWith(Transactional.sink(producerDefaults, transactionalId))
            }
            .toMat(Sink.ignore)(Keep.both)
            .run()

        log.info("Running 2 transactional workloads with prefix transactional id: {}", transactionalId)
        val (control1, streamResult1) = runTransactional()
        val (control2, streamResult2) = runTransactional()

        log.info("Waiting until partitions are assigned across both consumers")
        waitUntilConsumerSummary(group) {
          case consumer1 :: consumer2 :: Nil =>
            val half = maxPartitions / 2
            consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
        }

        log.info("Seeding topic with '{}' elements for all partitions ({})", elements, maxPartitions)
        Await.result(produceToAllPartitions(testProducerSettings, sourceTopic, maxPartitions, 1 to elements),
                     remainingOrDefault)

        val consumer = consumePartitionOffsetValues(
          probeConsumerSettings(createGroupId(2)),
          sinkTopic,
          elementsToTake = (elements * maxPartitions).toLong
        )

        log.info("Retrieve actual values")
        val actualValues: immutable.Seq[(Int, Long, String)] = Await.result(consumer, 60.seconds)

        log.info("Waiting until partitions are assigned to one non-failed consumer")
        waitUntilConsumerSummary(group) {
          case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == maxPartitions
        }

        log.info("Assert that one of the stream results has failed")
        if (streamResult1.isCompleted)
          recoverToSucceededIf[Exception](streamResult1)
        else if (streamResult2.isCompleted)
          recoverToSucceededIf[Exception](streamResult2)
        else fail("Expected one of the stream results to have failed")

        assertPartitionedConsistency(elements, maxPartitions, actualValues)

        Await.result(control1.shutdown(), remainingOrDefault)
        Await.result(control2.shutdown(), remainingOrDefault)
      }
    }

    "rebalance safely using transactional partitioned flow" in assertAllStagesStopped {
      val partitions = 4
      val totalMessages = 200L

      val topic = createTopic(1, partitions)
      val outTopic = createTopic(2, partitions)
      val group = createGroupId(1)
      val transactionalId = createTransactionalId()
      val sourceSettings = consumerDefaults
        .withGroupId(group)

      val topicSubscription = Subscriptions.topics(topic)

      def createAndRunTransactionalFlow(subscription: AutoSubscription) =
        Transactional
          .partitionedSource(sourceSettings, subscription)
          .map {
            case (tp, source) =>
              source
                .map { msg =>
                  ProducerMessage.single(new ProducerRecord[String, String](outTopic,
                                                                            msg.record.partition(),
                                                                            msg.record.key(),
                                                                            msg.record.value() + "-out"),
                                         msg.partitionOffset)
                }
                .to(Transactional.sink(producerDefaults, transactionalId))
                .run()
          }
          .toMat(Sink.ignore)(DrainingControl.apply)
          .run()

      def createAndRunProducer(elements: immutable.Iterable[Long]) =
        Source(elements)
          .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
          .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

      val control = createAndRunTransactionalFlow(topicSubscription)

      // waits until all partitions are assigned to the single consumer
      waitUntilConsumerSummary(group) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == partitions
      }

      createAndRunProducer(0L until totalMessages / 2).futureValue

      // create another consumer with the same groupId to trigger re-balancing
      val control2 = createAndRunTransactionalFlow(topicSubscription)

      // waits until partitions are assigned across both consumers
      waitUntilConsumerSummary(group) {
        case consumer1 :: consumer2 :: Nil =>
          val half = partitions / 2
          consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
      }

      createAndRunProducer(totalMessages / 2 until totalMessages).futureValue

      val checkingGroup = createGroupId(2)

      val (counterQueue, counterCompletion) = Source
        .queue[String](8, OverflowStrategy.fail)
        .scan(0L)((c, _) => c + 1)
        .takeWhile(_ < totalMessages, inclusive = true)
        .toMat(Sink.last)(Keep.both)
        .run()

      val streamMessages = Consumer
        .plainSource[String, String](consumerDefaults
                                       .withGroupId(checkingGroup)
                                       .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                                     Subscriptions.topics(outTopic))
        .mapAsync(1)(el => counterQueue.offer(el.value()).map(_ => el))
        .scan(0L)((c, _) => c + 1)
        .toMat(Sink.last)(DrainingControl.apply)
        .run()

      counterCompletion.futureValue shouldBe totalMessages

      control.drainAndShutdown().futureValue
      control2.drainAndShutdown().futureValue
      streamMessages.drainAndShutdown().futureValue shouldBe totalMessages
    }
  }

  def probeConsumerSettings(groupId: String): ConsumerSettings[String, String] =
    withProbeConsumerSettings(consumerDefaults, groupId)

  override def producerDefaults: ProducerSettings[String, String] =
    withTestProducerSettings(super.producerDefaults)

  def txProducerDefaults: ProducerSettings[String, String] =
    withTransactionalProducerSettings(super.producerDefaults)
}
