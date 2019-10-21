/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.ProducerMessage.MultiMessage
import akka.kafka._
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.concurrent.duration._
import scala.util.Random

class RebalanceSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  final val Numbers = (1 to 5000).map(_.toString)
  final val partition1 = 1

  "Fetched records" must {

    // The `max.poll.records` controls how many records Kafka fetches internally during a poll.
    "actually show even if partition is revoked" in assertAllStagesStopped {
      val count = 200
      // de-coupling consecutive test runs with crossScalaVersions on Travis
      val topicSuffix = Random.nextInt()
      val topic1 = createTopic(topicSuffix, partitions = 2)
      val group1 = createGroupId(1)
      val consumerSettings = consumerDefaults
        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        .withGroupId(group1)

      produceToTwoPartitions(topic1, count).futureValue shouldBe Done

      // Subscribe to the topic (without demand)
      val probe1rebalanceActor = TestProbe()
      val probe1subscription = Subscriptions.topics(topic1).withRebalanceListener(probe1rebalanceActor.ref)
      val (control1, probe1) = Consumer
        .plainSource(consumerSettings, probe1subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await initial partition assignment
      probe1rebalanceActor.expectMsgClass(classOf[TopicPartitionsRevoked])
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription,
                                Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      // request all messages
      probe1.request(count * 2L)

      // Subscribe to the topic (without demand)
      val prove2rebalanceActor = TestProbe()
      val probe2subscription = Subscriptions.topics(topic1).withRebalanceListener(prove2rebalanceActor.ref)
      val (control2, probe2) = Consumer
        .plainSource(consumerSettings, probe2subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await a revoke to both consumers
      probe1rebalanceActor.expectMsg(
        TopicPartitionsRevoked(probe1subscription,
                               Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )
      prove2rebalanceActor.expectMsgClass(classOf[TopicPartitionsRevoked])

      // the rebalance finishes
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription, Set(new TopicPartition(topic1, partition0)))
      )
      prove2rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe2subscription, Set(new TopicPartition(topic1, partition1)))
      )

      val probe1messages = probe1.expectNextN(count + 1L)
      probe2.request(count.toLong)
      val probe2messages = probe2.expectNextN(count.toLong)

      val probe1messages0 = probe1messages.filter(_.partition() == partition0)
      val probe1messages1 = probe1messages.filter(_.partition() == partition1)

      if (probe1messages0.size == 1) { // this depending on which partition gets issued "first" during poll
        // this is the problematic message: even though partition 0 is balanced away the enqueued messages are issued
        probe1messages0 should have size (1)
        probe1messages1 should have size (count.toLong)

        probe2messages should have size (count.toLong)
      } else {
        // this is the problematic message: even though partition 0 is balanced away the enqueued messages are issued
        probe1messages0 should have size (count.toLong)
        probe1messages1 should have size (1)

        probe2messages should have size (count.toLong)
      }

      probe1.cancel()
      probe2.cancel()

      control1.isShutdown.futureValue shouldBe Done
      control2.isShutdown.futureValue shouldBe Done
    }
  }

  private def produceToTwoPartitions(topic1: String, count: Int) = {
    val producing = Source(Numbers.take(count))
      .map { n =>
        MultiMessage(List(
                       new ProducerRecord(topic1, partition0, DefaultKey, n + "-p0"),
                       new ProducerRecord(topic1, partition1, DefaultKey, n + "-p1")
                     ),
                     NotUsed)
      }
      .via(Producer.flexiFlow(producerDefaults, testProducer))
      .runWith(Sink.ignore)
    producing
  }
}
