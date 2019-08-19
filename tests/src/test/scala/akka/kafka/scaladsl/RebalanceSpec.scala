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
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.concurrent.duration._

class RebalanceSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  final val Numbers = (1 to 5000).map(_.toString)
  final val partition1 = 1

  "Fetched records" must {

    // The `max.poll.records` controls how many records Kafka fetches internally during a poll.
    "do actually show even if partition is revoked" in assertAllStagesStopped {
      val count = 200
      val topic1 = createTopic(1, partitions = 2)
      val group1 = createGroupId(1)
      val consumerSettings = consumerDefaults
        .withProperty("max.poll.records", "1")
        .withGroupId(group1)

      produceToTwoPartitions(topic1, count).futureValue shouldBe Done

      // Subscribe to the topic (without demand)
      val rebalanceActor1 = TestProbe()
      val subscription1 = Subscriptions.topics(topic1).withRebalanceListener(rebalanceActor1.ref)
      val (control1, probe1) = Consumer
        .plainSource(consumerSettings, subscription1)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await initial partition assignment
      rebalanceActor1.expectMsgClass(classOf[TopicPartitionsRevoked])
      rebalanceActor1.expectMsg(
        TopicPartitionsAssigned(subscription1,
                                Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      // request all messages
      probe1.request(count * 2L)

      // Subscribe to the topic (without demand)
      val rebalanceActor2 = TestProbe()
      val subscription2 = Subscriptions.topics(topic1).withRebalanceListener(rebalanceActor2.ref)
      val (control2, probe2) = Consumer
        .plainSource(consumerSettings, subscription2)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await a revoke to both consumers
      rebalanceActor1.expectMsg(
        TopicPartitionsRevoked(subscription1,
                               Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )
      rebalanceActor2.expectMsgClass(classOf[TopicPartitionsRevoked])

      // the rebalance finishes
      rebalanceActor1.expectMsg(TopicPartitionsAssigned(subscription1, Set(new TopicPartition(topic1, partition0))))
      rebalanceActor2.expectMsg(TopicPartitionsAssigned(subscription2, Set(new TopicPartition(topic1, partition1))))

      val messages1 = probe1.expectNextN(count + 1L)
      probe2.request(count.toLong)
      val messages2 = probe2.expectNextN(count.toLong)

      val messages1p0 = messages1.filter(_.partition() == partition0)
      val messages1p1 = messages1.filter(_.partition() == partition1)

//      messages1p0 shouldBe Symbol("empty")
      messages1p0 should have size (1)
      messages1p1 should have size (count.toLong)
      messages2 should have size (count.toLong)

      messages1p0 should have size (1)
      messages1p0.head.partition() shouldBe partition0

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
