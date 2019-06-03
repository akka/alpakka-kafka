/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.Done
import akka.kafka.KafkaPorts
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import net.manub.embeddedkafka.EmbeddedKafkaConfig

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class MultiConsumerSpec extends SpecBase(kafkaPort = KafkaPorts.MultiConsumerSpec) with EmbeddedKafkaLike {

  override def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "broker.id" -> "1",
                          "num.partitions" -> "3",
                          "offsets.topic.replication.factor" -> "1",
                          "offsets.topic.num.partitions" -> "3"
                        ))

  "Multiple consumer in a single consumer group" must {

    "together read all data from multiple topics" in assertAllStagesStopped {
      val topics = List(createTopicName(0), createTopicName(1), createTopicName(2))
      val group1 = createGroupId(1)

      // produce 10 batches of 10 elements to all topics
      val batches = 10
      val batchSize = 10
      Await.result(produceBatches(topics, batches, batchSize), remainingOrDefault)

      val consumerSettings = consumerDefaults.withGroupId(group1)

      val (control1, probe1) = createProbe(consumerSettings, topics: _*)
      val (control2, probe2) = createProbe(consumerSettings, topics: _*)

      val (expectedData, expectedCount) = batchMessagesExpected(topics, batches, batchSize)

      probe1.request(expectedCount)
      probe2.request(expectedCount)

      val seq1 = probe1.receiveWithin(10.seconds)
      val seq2 = probe2.receiveWithin(1.seconds)

      val allReceived = seq1 ++ seq2
      allReceived should have size (expectedCount)
      allReceived should contain theSameElementsAs (expectedData)

      // Consumers are not fair, most of the time one receives all
      // seq1 should not be Symbol("empty")
      // seq2 should not be Symbol("empty")

      Await.result(Future.sequence(Seq(control1.shutdown(), control2.shutdown())), remainingOrDefault)
    }
  }

  "Consumer in different consumer groups" must {

    "read all data from multiple topics" in assertAllStagesStopped {
      val topics = List(createTopicName(0), createTopicName(1), createTopicName(2))
      val group1 = createGroupId(1)
      val group2 = createGroupId(2)

      // produce 10 batches of 10 elements to all topics
      val batches = 10
      val batchSize = 10
      Await.result(produceBatches(topics, batches, batchSize), remainingOrDefault)

      val consumerSettings1 = consumerDefaults.withGroupId(group1)
      val consumerSettings2 = consumerDefaults.withGroupId(group2)

      val (control1, probe1) = createProbe(consumerSettings1, topics: _*)
      val (control2, probe2) = createProbe(consumerSettings2, topics: _*)

      val (expectedData, expectedCount) = batchMessagesExpected(topics, batches, batchSize)
      probe1.request(expectedCount + 1)
      probe2.request(expectedCount + 1)
      probe1.expectNextN(expectedCount) should contain theSameElementsAs (expectedData)
      probe2.expectNextN(expectedCount) should contain theSameElementsAs (expectedData)

      probe1.expectNoMessage(1.seconds)
      probe2.expectNoMessage(1.seconds)

      Await.result(Future.sequence(Seq(control1.shutdown(), control2.shutdown())), remainingOrDefault)
    }

    "read all data from multiple topics in multiple partitions" in assertAllStagesStopped {
      val topics = List(createTopicName(0), createTopicName(1), createTopicName(2))
      val group1 = createGroupId(1)
      val group2 = createGroupId(2)

      // produce 10 batches of 10 elements to all topics on different partitions
      val batches = 10
      val batchSize = 10
      val produceMessages: immutable.Seq[Future[Done]] = (0 until batches)
        .flatMap { batch =>
          topics.map { topic =>
            val batchStart = batch * batchSize
            val values = (batchStart until batchStart + batchSize).map(i => topic + i.toString)
            produceString(topic, values, partition = batch % 3)
          }
        }
      Await.result(Future.sequence(produceMessages), remainingOrDefault)

      val consumerSettings1 = consumerDefaults.withGroupId(group1)
      val consumerSettings2 = consumerDefaults.withGroupId(group2)

      val (control1, probe1) = createProbe(consumerSettings1, topics: _*)
      val (control2, probe2) = createProbe(consumerSettings2, topics: _*)

      val (expectedData, expectedCount) = batchMessagesExpected(topics, batches, batchSize)
      probe1.request(expectedCount + 1)
      probe2.request(expectedCount + 1)
      probe1.expectNextN(expectedCount) should contain theSameElementsAs (expectedData)
      probe2.expectNextN(expectedCount) should contain theSameElementsAs (expectedData)

      probe1.expectNoMessage(1.seconds)
      probe2.expectNoMessage(1.seconds)

      Await.result(Future.sequence(Seq(control1.shutdown(), control2.shutdown())), remainingOrDefault)
    }

  }

}
