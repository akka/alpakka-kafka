/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.Done
import akka.kafka._
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import org.apache.kafka.clients.consumer.ConsumerConfig
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
    // issue explained in https://github.com/akka/alpakka-kafka/issues/872
    // this test added with https://github.com/akka/alpakka-kafka/pull/865
    "actually show even if partition is revoked" in assertAllStagesStopped {
      val count = 20L
      // de-coupling consecutive test runs with crossScalaVersions on Travis
      val topicSuffix = Random.nextInt()
      val topic1 = createTopic(topicSuffix, partitions = 2)
      val group1 = createGroupId(1)
      val consumerSettings = consumerDefaults
        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500") // 500 is the default value
        .withGroupId(group1)

      awaitProduce(produce(topic1, 0 to count.toInt, partition1))

      // Subscribe to the topic (without demand)
      val probe1rebalanceActor = TestProbe()
      val probe1subscription = Subscriptions.topics(topic1).withRebalanceListener(probe1rebalanceActor.ref)
      val (control1, probe1) = Consumer
        .plainSource(consumerSettings, probe1subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await initial partition assignment
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription,
                                Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      // read one message from probe1 with partition 1
      probe1.requestNext()

      // Subscribe to the topic (without demand)
      val probe2rebalanceActor = TestProbe()
      val probe2subscription = Subscriptions.topics(topic1).withRebalanceListener(probe2rebalanceActor.ref)
      val (control2, probe2) = Consumer
        .plainSource(consumerSettings, probe2subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // Await a revoke to consumer 1
      probe1rebalanceActor.expectMsg(
        TopicPartitionsRevoked(probe1subscription,
                               Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      // the rebalance finishes
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription, Set(new TopicPartition(topic1, partition0)))
      )
      probe2rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe2subscription, Set(new TopicPartition(topic1, partition1)))
      )

      probe1.request(count)
      probe2.request(count)

      val probe2messages = probe2.expectNextN(count)

      // no further messages enqueued on probe1 as partition 1 is balanced away
      probe1.expectNoMessage(500.millis)

      probe2messages should have size (count)

      probe1.cancel()
      probe2.cancel()

      control1.isShutdown.futureValue shouldBe Done
      control2.isShutdown.futureValue shouldBe Done
    }
  }
}
