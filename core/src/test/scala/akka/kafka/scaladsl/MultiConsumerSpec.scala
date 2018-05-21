/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.TimeUnit

import akka.Done
import akka.kafka.Subscriptions.TopicSubscription
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.test.Utils._
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.reflect.io.Directory

class MultiConsumerSpec extends SpecBase(kafkaPort = 9032) {

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort, zooKeeperPort,
      Map(
        "broker.id" -> "1",
        "num.partitions" -> "3",
        "offsets.topic.replication.factor" -> "1",
        "offsets.topic.num.partitions" -> "3"
      ))

  "A single consumer group" must {

    "read all data from multiple topics" in assertAllStagesStopped {
      val topics = List(
        createTopic(0),
        createTopic(1),
        createTopic(2))
      val group1 = createGroup(1)
      val group2 = createGroup(2)

      // produce 10 batches of 10 elements to all topics
      val produceMessages: immutable.Seq[Future[Done]] = (0 to 9).flatMap(batch => topics.map(t => {
        val values = ((batch * 10) to (batch * 10 + 9)).map(i => t + i.toString)
        produceString(t, values, partition = partition0)
      }))
      Await.result(Future.sequence(produceMessages), remainingOrDefault)

      val consumerSettings1 = consumerDefaults.withGroupId(group1)

      val (control1, probe1) = createProbe(consumerSettings1, topics: _*)
      val (control2, probe2) = createProbe(consumerSettings1, topics: _*)

      val expectedData = (0 to 9).flatMap(batch => topics.flatMap(t => ((batch * 10) to (batch * 10 + 9)).map(i => t + i.toString)))
      val expectedCount = 100 * topics.length

      probe1.request(expectedCount)
      probe2.request(expectedCount)

      val seq1 = probe1.receiveWithin(10.seconds)
      val seq2 = probe2.receiveWithin(10.seconds)

      seq1 should not be ('empty)
      seq2 should not Be ('empty)

      val allReceived = seq1 ++ seq2
      allReceived should have size (expectedCount)
      allReceived should contain theSameElementsAs (expectedData)

      Await.result(Future.sequence(Seq(control1.shutdown(), control2.shutdown())), remainingOrDefault)
    }
  }

  "Multiple consumer groups" must {

    "read all data from multiple topics" in assertAllStagesStopped {
      val topics = List(
        createTopic(0),
        createTopic(1),
        createTopic(2))
      val group1 = createGroup(1)
      val group2 = createGroup(2)

      // produce 10 batches of 10 elements to all topics
      val produceMessages: immutable.Seq[Future[Done]] = (0 to 9).flatMap(batch => topics.map(t => {
        val values = ((batch * 10) to (batch * 10 + 9)).map(i => t + i.toString)
        produceString(t, values, partition = partition0)
      }))
      Await.result(Future.sequence(produceMessages), remainingOrDefault)

      val consumerSettings1 = consumerDefaults.withGroupId(group1)
      val consumerSettings2 = consumerDefaults.withGroupId(group2)

      val (control1, probe1) = createProbe(consumerSettings1, topics: _*)
      val (control2, probe2) = createProbe(consumerSettings2, topics: _*)

      val expectedData = (0 to 9).flatMap(batch => topics.flatMap(t => ((batch * 10) to (batch * 10 + 9)).map(i => t + i.toString)))
      val expectedCount = 100 * topics.length
      probe1.request(expectedCount + 1)
      probe2.request(expectedCount + 1)
      probe1.expectNextN(expectedCount) should contain theSameElementsAs (expectedData)
      probe2.expectNextN(expectedCount) should contain theSameElementsAs (expectedData)

      probe1.expectNoMessage(1.seconds)
      probe2.expectNoMessage(1.seconds)

      Await.result(Future.sequence(Seq(control1.shutdown(), control2.shutdown())), remainingOrDefault)
    }

    "read all data from multiple topics in multiple partitions" in assertAllStagesStopped {
      val topics = List(
        createTopic(0),
        createTopic(1),
        createTopic(2))
      val group1 = createGroup(1)
      val group2 = createGroup(2)

      // produce 10 batches of 10 elements to all topics
      val produceMessages: immutable.Seq[Future[Done]] = (0 to 9).flatMap(batch => topics.map(t => {
        val values = ((batch * 10) to (batch * 10 + 9)).map(i => t + i.toString)
        produceString(t, values, partition = batch % 3)
      }))
      Await.result(Future.sequence(produceMessages), remainingOrDefault)

      val consumerSettings1 = consumerDefaults.withGroupId(group1)
      val consumerSettings2 = consumerDefaults.withGroupId(group2)

      val (control1, probe1) = createProbe(consumerSettings1, topics: _*)
      val (control2, probe2) = createProbe(consumerSettings2, topics: _*)

      val expectedData = (0 to 9).flatMap(batch => topics.flatMap(t => ((batch * 10) to (batch * 10 + 9)).map(i => t + i.toString)))
      val expectedCount = 100 * topics.length
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
