/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.kafka.ConsumerMessage.PartitionOffset
import akka.kafka.{ProducerMessage, _}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.testkit.scaladsl.{TestcontainersKafkaLike}
import akka.stream.{Attributes, DelayOverflowStrategy, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class TransactionsSpec extends SpecBase with TestcontainersKafkaLike with Repeated {

  "A consume-transform-produce cycle" must {

    "complete in happy-path scenario" in {
      assertAllStagesStopped {
        val sourceTopic = createTopic(1)
        val sinkTopic = createTopic(2)
        val group = createGroupId(1)

        Await.result(produce(sourceTopic, 1 to 100), remainingOrDefault)

        val consumerSettings = consumerDefaults.withGroupId(group)

        val control = transactionalCopyStream(consumerSettings, sourceTopic, sinkTopic, group, Int.MaxValue, 10.seconds)
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
          transactionalCopyStream(consumerSettings, sourceTopic, sinkTopic, s"$group-$id", Int.MaxValue, 10.seconds)
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

    "provide consistency when multiple transactional streams are being restarted" in assertAllStagesStopped {
      val sourcePartitions = 10
      val destinationPartitions = 4
      val consumers = 3

      val sourceTopic = createTopic(1, sourcePartitions)
      val sinkTopic = createTopic(2, destinationPartitions)
      val group = createGroupId(1)

      val elements = 100 * 1000
      val restartAfter = 10 * 1000

      val partitionSize = elements / sourcePartitions
      val producers =
        (0 until sourcePartitions).map(
          part => produce(sourceTopic, ((part * partitionSize) + 1) to (partitionSize * (part + 1)), part)
        )
      Await.result(Future.sequence(producers), 1.minute)

      val consumerSettings = consumerDefaults.withGroupId(group)

      val completedCopy = new AtomicInteger(0)
      val completedWithTimeout = new AtomicInteger(0)

      def runStream(id: String): UniqueKillSwitch =
        RestartSource
          .onFailuresWithBackoff(10.millis, 100.millis, 0.2)(
            () => {
              val transactionId = s"$group-$id"
              transactionalCopyStream(consumerSettings, sourceTopic, sinkTopic, transactionId, restartAfter, 10.seconds)
                .recover {
                  case e: TimeoutException =>
                    if (completedWithTimeout.incrementAndGet() > 10)
                      "no more messages to copy"
                    else
                      throw new Error("Continue restarting copy stream")
                }
            }
          )
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.onComplete {
            case Success(_) =>
              completedCopy.incrementAndGet()
            case Failure(_) => // restart
          })(Keep.left)
          .run()

      val controls: Seq[UniqueKillSwitch] = (0 until consumers)
        .map(_.toString)
        .map(runStream)

      val probeConsumerGroup = createGroupId(2)

      while (completedCopy.get() < consumers) {
        Thread.sleep(2000)
      }

      retry(3) { n =>
        val consumer = offsetValueSource(probeConsumerSettings(probeConsumerGroup), sinkTopic)
          .take(elements.toLong)
          .idleTimeout(30.seconds)
          .alsoTo(
            Flow[(Long, String)]
              .scan(0) { case (count, _) => count + 1 }
              .filter(_ % 10000 == 0)
              .log("received")
              .to(Sink.ignore)
          )
          .recover {
            case t => (0, "no-more-elements")
          }
          .filter(_._2 != "no-more-elements")
          .runWith(Sink.seq)
        val values = Await.result(consumer, 10.minutes)

        val expected = (1 to elements).map(_.toString)
        withClue("Checking for duplicates: ") {
          val duplicates = values.map(_._2) diff expected
          if (duplicates.nonEmpty) {
            val duplicatesWithDifferentOffsets = values
              .filter {
                case (_, value) => duplicates.contains(value)
              }
              .groupBy(_._2) // message
              .mapValues(_.map(_._1)) // keep offset
              .filter {
                case (_, offsets) => offsets.distinct.size > 1
              }

            if (duplicatesWithDifferentOffsets.nonEmpty) {
              fail(s"Got ${duplicates.size} duplicates. Messages and their offsets: $duplicatesWithDifferentOffsets")
            } else {
              println("Got duplicates, but all of them were due to rebalance replay when counting")
            }
          }
        }
        withClue("Checking for missing: ") {
          val missing = expected diff values.map(_._2)
          if (missing.nonEmpty) {
            val continuousBlocks = missing
              .scanLeft(("-1", 0)) {
                case ((last, block), curr) => if (last.toInt + 1 == curr.toInt) (curr, block) else (curr, block + 1)
              }
              .tail
              .groupBy(_._2)
            val blockDescription = continuousBlocks
              .map { block =>
                val msgs = block._2.map(_._1)
                s"Missing ${msgs.size} in continuous block, first ten: ${msgs.take(10)}"
              }
              .mkString(" ")
            fail(s"Did not get ${missing.size} expected messages. $blockDescription")
          }
        }
      }
      controls.map(_.shutdown())
    }

    // Returning T, throwing the exception on failure
    @annotation.tailrec
    def retry[T](n: Int)(fn: Int => T): T =
      util.Try { fn(n + 1) } match {
        case util.Success(x) => x
        case _ if n > 1 => retry(n - 1)(fn)
        case util.Failure(e) => throw e
      }

    "drain stream on partitions rebalancing" in assertAllStagesStopped {
      // Runs a copying transactional flows that delay writing to the output partition using a `delay` stage.
      // Creates more flows than ktps to trigger partition rebalancing.
      // The output topic should contain the same elements as the input topic.

      val sourceTopic = createTopic(1, partitions = 2)
      val sinkTopic = createTopic(2, partitions = 4)
      val group = createGroupId(1)

      val elements = 100
      val batchSize = 10
      Await.result(produce(sourceTopic, 1 to elements), remainingOrDefault)

      val elementsWrote = new AtomicInteger(0)

      val consumerSettings = consumerDefaults.withGroupId(group)

      def runStream(id: String): Consumer.Control = {
        val control: Control =
          Transactional
            .source(consumerSettings, Subscriptions.topics(sourceTopic))
            .map { msg =>
              ProducerMessage.single(new ProducerRecord[String, String](sinkTopic, msg.record.value),
                                     msg.partitionOffset)
            }
            .take(batchSize.toLong)
            .delay(3.seconds, strategy = DelayOverflowStrategy.backpressure)
            .addAttributes(Attributes.inputBuffer(batchSize, batchSize + 1))
            .via(Transactional.flow(producerDefaults, s"$group-$id"))
            .map(_ => elementsWrote.incrementAndGet())
            .toMat(Sink.ignore)(Keep.left)
            .run()
        control
      }

      val controls: Seq[Control] = (0 until elements / batchSize)
        .map(_.toString)
        .map(runStream)

      val probeConsumerGroup = createGroupId(2)
      val probeConsumer = valuesProbeConsumer(probeConsumerSettings(probeConsumerGroup), sinkTopic)

      periodicalCheck("Wait for elements written to Kafka", maxTries = 30, 1.second) { () =>
        elementsWrote.get()
      }(_ > 10)

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
        .expectNextN((1 to 100).grouped(10).map(_.sum.toString).to[immutable.Seq])

      val concatsConsumer = valuesProbeConsumer(probeConsumerSettings(createGroupId(2)), concatsTopic)

      concatsConsumer
        .request(10)
        .expectNextN((1 to 100).map(_.toString).grouped(10).map(_.reduce(_ + _)).to[immutable.Seq])

      sumsConsumer.cancel()
      concatsConsumer.cancel()
      Await.result(control.shutdown(), remainingOrDefault)
    }
  }

  private def transactionalCopyStream(
      consumerSettings: ConsumerSettings[String, String],
      sourceTopic: String,
      sinkTopic: String,
      transactionalId: String,
      restartAfter: Int,
      idleTimeout: FiniteDuration
  ): Source[ProducerMessage.Results[String, String, PartitionOffset], Control] =
    Transactional
      .source(consumerSettings, Subscriptions.topics(sourceTopic))
      .zip(Source.unfold(1)(count => Some((count + 1, count))))
      .map {
        case (msg, count) =>
          if (count >= restartAfter) throw new Error("Restarting transactional copy stream")
          msg
      }
      .idleTimeout(idleTimeout)
      .map { msg =>
        ProducerMessage.single(new ProducerRecord[String, String](sinkTopic, msg.record.value), msg.partitionOffset)
      }
      .via(Transactional.flow(producerDefaults, transactionalId))

  private def probeConsumerSettings(groupId: String): ConsumerSettings[String, String] =
    consumerDefaults
      .withGroupId(groupId)
      .withProperties(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")

  private def valuesProbeConsumer(settings: ConsumerSettings[String, String],
                                  topic: String): TestSubscriber.Probe[String] =
    offsetValueSource(settings, topic)
      .map(_._2)
      .runWith(TestSink.probe)

  private def offsetValueSource(settings: ConsumerSettings[String, String],
                                topic: String): Source[(Long, String), Consumer.Control] =
    Consumer
      .plainSource(settings, Subscriptions.topics(topic))
      .map(r => (r.offset(), r.value()))

  override def producerDefaults: ProducerSettings[String, String] =
    super.producerDefaults
      .withParallelism(20)
      .withCloseTimeout(Duration.Zero)
}
