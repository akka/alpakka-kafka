/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.kafka.scaladsl.SpecBase
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.scalatest.concurrent.PatienceConfiguration.Interval
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.Ignore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success}

@Ignore
class TransactionsPartitionedSourceSpec
    extends SpecBase
    with TestcontainersKafkaPerClassLike
    with AnyWordSpecLike
    with ScalaFutures
    with Matchers
    with TransactionsOps
    with Repeated {

  val replicationFactor = 2

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(45.seconds, 1.second)

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system)
    .withNumBrokers(3)
    .withInternalTopicsReplicationFactor(replicationFactor)

  "A multi-broker consume-transform-produce cycle" must {
    "provide consistency when multiple partitioned transactional streams are being restarted" in assertAllStagesStopped {
      val sourcePartitions = 4
      val destinationPartitions = 4
      val consumers = 3
      val replication = replicationFactor

      val sourceTopic = createTopic(1, sourcePartitions, replication)
      val sinkTopic = createTopic(2, destinationPartitions, replication)
      val group = createGroupId(1)
      val transactionalId = createTransactionalId()

      val elements = 100 * 1000 // 100 * 1,000 = 100,000
      val restartAfter = (10 * 1000) / sourcePartitions // (10 * 1,000) / 10 = 100

      val producers: immutable.Seq[Future[Done]] =
        (0 until sourcePartitions).map { part =>
          produce(sourceTopic, range = 1 to elements, partition = part)
        }

      Await.result(Future.sequence(producers), 4.minute)

      val consumerSettings = consumerDefaults.withGroupId(group)

      val completedCopy = new AtomicInteger(0)
      val completedWithTimeout = new AtomicInteger(0)

      def runStream(id: String): UniqueKillSwitch =
        RestartSource
          .onFailuresWithBackoff(RestartSettings(10.millis, 100.millis, 0.2))(
            () => {
              transactionalPartitionedCopyStream(
                consumerSettings,
                txProducerDefaults,
                sourceTopic,
                sinkTopic,
                transactionalId,
                idleTimeout = 10.seconds,
                maxPartitions = sourcePartitions,
                restartAfter = Some(restartAfter)
              ).recover {
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

      eventually(Interval(2.seconds)) {
        completedCopy.get() should be < consumers
      }

      val consumer = consumePartitionOffsetValues(
        probeConsumerSettings(createGroupId(2)),
        sinkTopic,
        elementsToTake = (elements * destinationPartitions).toLong
      )

      val actualValues = Await.result(consumer, 10.minutes)

      log.debug("Expected elements: {}, actual elements: {}", elements, actualValues.length)

      assertPartitionedConsistency(elements, destinationPartitions, actualValues)

      controls.foreach(_.shutdown())
    }
  }

  private def probeConsumerSettings(groupId: String): ConsumerSettings[String, String] =
    withProbeConsumerSettings(consumerDefaults, groupId)

  override def producerDefaults: ProducerSettings[String, String] =
    withTestProducerSettings(super.producerDefaults)

  def txProducerDefaults: ProducerSettings[String, String] =
    withTransactionalProducerSettings(super.producerDefaults)
}
