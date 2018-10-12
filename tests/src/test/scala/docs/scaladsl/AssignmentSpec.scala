/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.kafka.scaladsl.{Consumer, Producer, SpecBase}
import akka.kafka.{KafkaPorts, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._

class AssignmentSpec extends SpecBase(kafkaPort = KafkaPorts.AssignmentSpec) {

  implicit val patience = PatienceConfig(15.seconds, 500.millis)

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> "1"
                        ))

  "subscription with partition assignment" must {

    "consume from the specified partition" in assertAllStagesStopped {
      val topic = createTopic(partitions = 2)
      val totalMessages = 100
      val producerCompletion =
        Source(1 to totalMessages)
          .map { msg =>
            val partition = msg % 2
            new ProducerRecord(topic, partition, DefaultKey, msg.toString)
          }
          .runWith(Producer.plainSink(producerDefaults))

      producerCompletion.futureValue

      // #assingment-single-partition
      val partition = 0
      val subscription = Subscriptions.assignment(new TopicPartition(topic, partition))
      val consumer = Consumer.plainSource(consumerDefaults, subscription)
      // #assingment-single-partition

      val messages = consumer.take(totalMessages / 2).map(_.value().toInt).runWith(Sink.seq)
      messages.futureValue.map(_ % 2 shouldBe 0)
    }

    "consume from the specified partition and offset" in assertAllStagesStopped {
      val topic = createTopic(partitions = 1)
      val totalMessages = 100
      val producerCompletion =
        Source(1 to totalMessages)
          .map { msg =>
            new ProducerRecord(topic, 0, DefaultKey, msg.toString)
          }
          .runWith(Producer.plainSink(producerDefaults))

      producerCompletion.futureValue

      // #assingment-single-partition-offset
      val partition = 0
      val offset: Long = totalMessages / 2
      val subscription = Subscriptions.assignmentWithOffset(new TopicPartition(topic, partition) -> offset)
      val consumer = Consumer.plainSource(consumerDefaults, subscription)
      // #assingment-single-partition-offset

      val messages = consumer.take(totalMessages / 2).map(_.offset()).runWith(Sink.seq)
      messages.futureValue.map(_ - offset).zipWithIndex.map { case (offs, idx) => offs - idx }.sum shouldBe 0
    }
  }

}
