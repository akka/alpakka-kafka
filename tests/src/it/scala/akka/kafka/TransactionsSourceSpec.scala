/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, SpecBase, Transactional}
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.concurrent.PatienceConfiguration.Interval
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success}

class TransactionsSourceSpec
    extends SpecBase
    with TestcontainersKafkaPerClassLike
    with WordSpecLike
    with ScalaFutures
    with Matchers
    with TransactionsOps
    with Repeated {

  // It's possible to get into a livelock situation where the `restartAfter` interval causes transactions to abort
  // over and over.  This can happen when there are a few partitions left to process and they can never be fully
  // processed because we always restart the stream before the transaction can be completed successfully.
  // The `maxRestarts` provides an upper bound for the maximum number of times we restart the stream so if we get
  // into a livelock it can eventually be resolved by not restarting any more.
  val maxRestarts = new AtomicInteger(1000)

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(45.seconds, 1.second)

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system)
    .withNumBrokers(3)
    .withInternalTopicsReplicationFactor(2)

  "A multi-broker consume-transform-produce cycle" must {
    "provide consistency when multiple transactional streams are being restarted" in assertAllStagesStopped {
      val sourcePartitions = 10
      val destinationPartitions = 4
      val consumers = 3
      val replication = 2

      val sourceTopic = createTopic(1, sourcePartitions, replication)
      val sinkTopic = createTopic(2, destinationPartitions, replication)
      val group = createGroupId(1)

      val elements = 100 * 1000
      val restartAfter = 10 * 1000

      val partitionSize = elements / sourcePartitions
      val producers: immutable.Seq[Future[Done]] =
        (0 until sourcePartitions).map { part =>
          val rangeStart = ((part * partitionSize) + 1)
          val rangeEnd = (partitionSize * (part + 1))
          log.info(s"Producing [$rangeStart to $rangeEnd] to partition $part")
          produce(sourceTopic, rangeStart to rangeEnd, part)
        }

      Await.result(Future.sequence(producers), 1.minute)

      val consumerSettings = consumerDefaults.withGroupId(group)

      val completedCopy = new AtomicInteger(0)
      val completedWithTimeout = new AtomicInteger(0)

      def runStream(id: String): UniqueKillSwitch =
        RestartSource
          .onFailuresWithBackoff(10.millis, 100.millis, 0.2)(
            () => {
              val transactionId = s"$group-$id"
              transactionalCopyStream(consumerSettings,
                                      txProducerDefaults,
                                      sourceTopic,
                                      sinkTopic,
                                      transactionId,
                                      10.seconds,
                                      Some(restartAfter),
                                      Some(maxRestarts))
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

      eventually(Interval(2.seconds)) {
        completedCopy.get() should be < consumers
      }

      val consumer = offsetValueSource(probeConsumerSettings(probeConsumerGroup), sinkTopic)
        .take(elements.toLong)
        .alsoTo(
          Flow[(Long, String)]
            .scan(0) { case (count, _) => count + 1 }
            .filter(_ % 10000 == 0)
            .log("received")
            .to(Sink.ignore)
        )
        .recover {
          case t => (0L, "no-more-elements")
        }
        .filter(_._2 != "no-more-elements")
        .runWith(Sink.seq)

      val values = Await.result(consumer, 10.minutes)

      val expected = (1 to elements).map(_.toString)

      log.info("Expected elements: {}, actual elements: {}", elements, values.length)

      checkForMissing(values, expected)
      checkForDuplicates(values, expected)

      controls.foreach(_.shutdown())
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
  }

  private def probeConsumerSettings(groupId: String): ConsumerSettings[String, String] =
    withProbeConsumerSettings(consumerDefaults, groupId)

  override def producerDefaults: ProducerSettings[String, String] =
    withTestProducerSettings(super.producerDefaults)

  def txProducerDefaults: ProducerSettings[String, String] =
    withTransactionalProducerSettings(super.producerDefaults)
}
