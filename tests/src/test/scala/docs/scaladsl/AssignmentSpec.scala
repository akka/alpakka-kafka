/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.{Consumer, Producer, SpecBase}
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable

class AssignmentSpec extends SpecBase with TestcontainersKafkaLike {

  "subscription with partition assignment" must {

    "consume from the specified single topic" in assertAllStagesStopped {
      val topic = createTopic()
      val group = createGroupId()
      val totalMessages = 100
      val producerCompletion =
        Source(1 to totalMessages)
          .map { msg =>
            new ProducerRecord(topic, 0, DefaultKey, msg.toString)
          }
          .runWith(Producer.plainSink(producerDefaults))

      producerCompletion.futureValue

      // #single-topic
      val subscription = Subscriptions.topics(topic)
      val consumer = Consumer.plainSource(consumerDefaults.withGroupId(group), subscription)
      // #single-topic

      val messages =
        consumer.takeWhile(_.value().toInt < totalMessages, inclusive = true).runWith(Sink.seq)
      messages.futureValue.size shouldBe totalMessages
    }

    "consume from the specified topic pattern" in assertAllStagesStopped {
      val suffix = (System.currentTimeMillis() % 10000).toInt

      val topics = immutable.Seq(createTopic(suffix), createTopic(suffix))
      val group = createGroupId()
      val totalMessages = 100
      val producerCompletion =
        Source(1 to totalMessages)
          .mapConcat { msg =>
            topics.map(topic => new ProducerRecord(topic, 0, DefaultKey, msg.toString))
          }
          .runWith(Producer.plainSink(producerDefaults))

      producerCompletion.futureValue shouldBe Done

      // #topic-pattern
      val pattern = s"topic-$suffix-[0-9]+"
      val subscription = Subscriptions.topicPattern(pattern)
      val consumer = Consumer.plainSource(consumerDefaults.withGroupId(group), subscription)
      // #topic-pattern

      val messages =
        consumer
          .take(totalMessages * topics.length.toLong)
          .runWith(Sink.seq)
      val received = messages.futureValue
      if (received.size != totalMessages * topics.size) {
        received.foreach(r => println(s"topic=${r.topic()} value=${r.value()}"))
      }
      received.size shouldBe totalMessages * topics.size
    }

    "consume from the specified partition" in assertAllStagesStopped {
      val topic = createTopic(suffix = 0, partitions = 2)
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

      val messages = consumer.take(totalMessages.toLong / 2).map(_.value().toInt).runWith(Sink.seq)
      messages.futureValue.map(_ % 2 shouldBe 0)
    }

    "consume from the specified partition and offset" in assertAllStagesStopped {
      val topic = createTopic()
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
      val offset: Long = totalMessages.toLong / 2
      val subscription = Subscriptions.assignmentWithOffset(new TopicPartition(topic, partition) -> offset)
      val consumer = Consumer.plainSource(consumerDefaults, subscription)
      // #assingment-single-partition-offset

      val messages = consumer.take(totalMessages.toLong / 2).map(_.offset()).runWith(Sink.seq)
      messages.futureValue.map(_ - offset).zipWithIndex.map { case (offs, idx) => offs - idx }.sum shouldBe 0
    }

    "consume from the specified partition and timestamp" in assertAllStagesStopped {
      val topic = createTopic()
      val totalMessages = 100
      val producerCompletion =
        Source(1 to totalMessages)
          .map { msg =>
            new ProducerRecord(topic, 0, System.currentTimeMillis(), DefaultKey, msg.toString)
          }
          .runWith(Producer.plainSink(producerDefaults))

      producerCompletion.futureValue

      // #assingment-single-partition-timestamp
      val partition = 0
      val now = System.currentTimeMillis
      val messagesSince: Long = now - 5000
      val subscription = Subscriptions.assignmentOffsetsForTimes(new TopicPartition(topic, partition) -> messagesSince)
      val consumer = Consumer.plainSource(consumerDefaults, subscription)
      // #assingment-single-partition-timestamp

      val messages =
        consumer.takeWhile(_.value().toInt < totalMessages, inclusive = true).map(_.timestamp()).runWith(Sink.seq)
      messages.futureValue.map(_ - now).count(_ > 5000) shouldBe 0
    }
  }

  "illegal subscriptions" should {
    "fail the stream with invalid topic" in {
      val streamCompletion = Consumer
        .plainSource(consumerDefaults.withGroupId(createGroupId()), Subscriptions.topics("illegal topic name"))
        .runWith(Sink.ignore)
      streamCompletion.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]
    }

    "fail the stream for empty subscriptions list" in {
      val streamCompletion = Consumer
        .plainSource(consumerDefaults.withGroupId(createGroupId()), Subscriptions.topics())
        .runWith(Sink.ignore)
      streamCompletion.failed.futureValue shouldBe a[java.lang.IllegalStateException]
    }

    "fail the stream for `null` topic name" in {
      val streamCompletion = Consumer
        .plainSource(consumerDefaults.withGroupId(createGroupId()), Subscriptions.topics(Set(null: String)))
        .runWith(Sink.ignore)
      streamCompletion.failed.futureValue shouldBe a[java.lang.IllegalArgumentException]
    }

    "fail the stream with invalid topic for assignments" in {
      val streamCompletion = Consumer
        .plainSource(consumerDefaults.withGroupId(createGroupId()),
                     Subscriptions.assignment(new TopicPartition("illegal topic name", 2)))
        .runWith(Sink.ignore)
      streamCompletion.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]
    }

    "fail the stream for illegal topic pattern" in {
      val streamCompletion = Consumer
        .plainSource(consumerDefaults.withGroupId(createGroupId()), Subscriptions.topicPattern("illegal regex (*"))
        .runWith(Sink.ignore)
      streamCompletion.failed.futureValue shouldBe a[java.util.regex.PatternSyntaxException]
    }

    "fail the stream for illegal offsets for times" in {
      val streamCompletion = Consumer
        .plainSource(
          consumerDefaults.withGroupId(createGroupId()),
          Subscriptions.assignmentOffsetsForTimes(new TopicPartition("topic", 0) -> 232L,
                                                  new TopicPartition("topic", 1) -> -232L)
        )
        .runWith(Sink.ignore)
      streamCompletion.failed.futureValue shouldBe a[java.lang.IllegalArgumentException]
    }
  }

  // #testkit

}
// #testkit
