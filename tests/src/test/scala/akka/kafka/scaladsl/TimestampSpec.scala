/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.{KafkaPorts, Subscriptions}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.TopicPartition
import org.scalatest.Inside

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class TimestampSpec extends SpecBase(kafkaPort = KafkaPorts.TimestampSpec) with Inside {

  implicit val patience = PatienceConfig(5.second, 100.millis)

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> "1"
                        ))

  "Kafka connector" must {
    "begin consuming from the given timestamp of the topic" in {
      assertAllStagesStopped {
        val topic = createTopicName(1)
        val group = createGroupId(1)

        givenInitializedTopic(topic)

        val now = System.currentTimeMillis()
        Await.result(produceTimestamped(topic, (1 to 100).zip(now to (now + 100))), remainingOrDefault)

        val consumerSettings = consumerDefaults.withGroupId(group)
        val consumer = consumerSettings.createKafkaConsumer()
        val partitions = consumer.partitionsFor(topic).asScala.map { t =>
          new TopicPartition(t.topic(), t.partition())
        }
        val topicsAndTs = Subscriptions.assignmentOffsetsForTimes(partitions.map(_ -> (now + 50)): _*)

        val probe = Consumer
          .plainSource(consumerSettings, topicsAndTs)
          .filterNot(_.value == InitialMsg)
          .map(_.value())
          .runWith(TestSink.probe)

        probe
          .request(50)
          .expectNextN((51 to 100).map(_.toString))

        probe.cancel()
      }
    }

    "handle topic that has no messages by timestamp" in {
      assertAllStagesStopped {
        val topic = createTopicName(1)
        val group = createGroupId(1)

        givenInitializedTopic(topic)

        val now = System.currentTimeMillis()

        val consumerSettings = consumerDefaults.withGroupId(group)
        val consumer = consumerSettings.createKafkaConsumer()
        val partitions = consumer.partitionsFor(topic).asScala.map { t =>
          new TopicPartition(t.topic(), t.partition())
        }
        val topicsAndTs = Subscriptions.assignmentOffsetsForTimes(partitions.map(_ -> (now + 50)): _*)

        val probe = Consumer
          .plainSource(consumerSettings, topicsAndTs)
          .runWith(TestSink.probe)

        probe.ensureSubscription()
        probe.expectNoMessage(200.millis)
        probe.cancel()
      }
    }

    "handle non existing topic" in {
      assertAllStagesStopped {
        val group = createGroupId(1)

        val now = System.currentTimeMillis()

        val consumerSettings = consumerDefaults.withGroupId(group)
        val topicsAndTs = Subscriptions.assignmentOffsetsForTimes(new TopicPartition("non-existing-topic", 0) -> now)

        val probe = Consumer
          .plainSource(consumerSettings, topicsAndTs)
          .runWith(TestSink.probe)

        probe.ensureSubscription()
        probe.expectNoMessage(200.millis)
        probe.cancel()
      }
    }
  }
}
