/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Producer
import akka.kafka.scaladsl.SpecBase
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.TopicConfig
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.concurrent.duration._

class PartitionedSourceFailoverSpec
    extends SpecBase
    with TestcontainersKafkaPerClassLike
    with AnyWordSpecLike
    with ScalaFutures
    with Matchers {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(45.seconds, 1.second)

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system)
    .withNumBrokers(3)
    .withInternalTopicsReplicationFactor(2)

  "partitioned source" should {
    "not lose any messages when a Kafka node dies" in assertAllStagesStopped {
      val totalMessages = 1000 * 10L
      val partitions = 4

      // TODO: This is probably not necessary anymore since the testcontainer setup blocks until all brokers are online.
      // TODO: However it is nice reassurance to hear from Kafka itself that the cluster is formed.
      waitUntilCluster() {
        _.nodes().get().size == testcontainersSettings.numBrokers
      }

      val topic = createTopic(
        0,
        partitions,
        replication = 3,
        Map(
          // require at least two replicas be in sync before acknowledging produced record
          TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> "2"
        )
      )
      val groupId = createGroupId(0)

      val consumerConfig = consumerDefaults
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100") // default was 5 * 60 * 1000 (five minutes)

      val consumerMatValue: Future[Long] = Consumer
        .plainPartitionedSource(consumerConfig, Subscriptions.topics(topic))
        .groupBy(partitions, _._1)
        .mapAsync(8) {
          case (tp, source) =>
            log.info(s"Sub-source for ${tp}")
            source
              .scan(0L)((c, _) => c + 1)
              .via(IntegrationTests.logReceivedMessages(tp)(log))
              // shutdown substream after receiving at its share of the total messages
              .takeWhile(count => count < (totalMessages / partitions), inclusive = true)
              .runWith(Sink.last)
        }
        .mergeSubstreams
        // sum of sums. sum the last results of substreams.
        .scan(0L)((c, subValue) => c + subValue)
        .takeWhile(count => count < totalMessages, inclusive = true)
        .runWith(Sink.last)

      waitUntilConsumerGroup(groupId) {
        !_.members().isEmpty
      }

      val producerConfig = producerDefaults.withProperties(
        // require acknowledgement from at least min in sync replicas (2).  default is 1
        ProducerConfig.ACKS_CONFIG -> "all"
      )

      val result: Future[Done] = Source(1L to totalMessages)
        .via(IntegrationTests.logSentMessages()(log))
        .map { number =>
          if (number == totalMessages / 2) {
            IntegrationTests.stopRandomBroker(brokerContainers, number)(log)
          }
          number
        }
        .map(number => new ProducerRecord(topic, (number % partitions).toInt, DefaultKey, number.toString))
        .runWith(Producer.plainSink(producerConfig))

      result.futureValue shouldBe Done
      // wait for consumer to consume all up until totalMessages, or timeout
      val actualCount = consumerMatValue.futureValue
      log.info("Actual messages received [{}], total messages sent [{}]", actualCount, totalMessages)
      // assert that we receive at least the number of messages we sent, there could be more due to retries
      assert(actualCount >= totalMessages)
    }
  }
}
