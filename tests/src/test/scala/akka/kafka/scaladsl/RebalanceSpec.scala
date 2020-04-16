/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka._
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.kafka.tests.AlpakkaAssignor
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.concurrent.duration._
import scala.util.Random

class RebalanceSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  final val Numbers = (1 to 5000).map(_.toString)
  final val partition1 = 1
  final val consumerClientId1 = "consumer-1"
  final val consumerClientId2 = "consumer-2"

  "Fetched records" must {

    // The `max.poll.records` controls how many records Kafka fetches internally during a poll.
    // issue explained in https://github.com/akka/alpakka-kafka/issues/872
    // this test added with https://github.com/akka/alpakka-kafka/pull/865
    "be removed from the source stage buffer when a partition is revoked" in assertAllStagesStopped {
      val count = 20L
      // de-coupling consecutive test runs with crossScalaVersions on Travis
      val topicSuffix = Random.nextInt()
      val topic1 = createTopic(topicSuffix, partitions = 2)
      val group1 = createGroupId(1)
      val tp0 = new TopicPartition(topic1, partition0)
      val tp1 = new TopicPartition(topic1, partition1)
      val consumerSettings = consumerDefaults
        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500") // 500 is the default value
        .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[AlpakkaAssignor].getName)
        .withGroupId(group1)

      awaitProduce(produce(topic1, 0 to count.toInt, partition1))

      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(tp0, tp1)
        )
      )

      log.debug("Subscribe to the topic (without demand)")
      val probe1rebalanceActor = TestProbe()
      val probe1subscription = Subscriptions.topics(topic1).withRebalanceListener(probe1rebalanceActor.ref)
      val (control1, probe1) = Consumer
        .plainSource(consumerSettings.withClientId(consumerClientId1), probe1subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      log.debug("Await initial partition assignment")
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription,
                                Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      log.debug("read one message from probe1 with partition 1")
      probe1.requestNext()

      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(tp0),
          consumerClientId2 -> Set(tp1)
        )
      )

      log.debug("Subscribe to the topic (without demand)")
      val probe2rebalanceActor = TestProbe()
      val probe2subscription = Subscriptions.topics(topic1).withRebalanceListener(probe2rebalanceActor.ref)
      val (control2, probe2) = Consumer
        .plainSource(consumerSettings.withClientId(consumerClientId2), probe2subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      log.debug("Await a revoke to consumer 1")
      probe1rebalanceActor.expectMsg(
        TopicPartitionsRevoked(probe1subscription,
                               Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      log.debug("the rebalance finishes")
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription, Set(new TopicPartition(topic1, partition0)))
      )
      probe2rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe2subscription, Set(new TopicPartition(topic1, partition1)))
      )

      log.debug("resume demand on both consumers")
      probe1.request(count)
      probe2.request(count)

      val probe2messages = probe2.expectNextN(count)

      log.debug("no further messages enqueued on probe1 as partition 1 is balanced away")
      probe1.expectNoMessage(500.millis)

      probe2messages should have size count

      probe1.cancel()
      probe2.cancel()

      control1.isShutdown.futureValue shouldBe Done
      control2.isShutdown.futureValue shouldBe Done
    }

    "be removed from the partitioned source stage buffer when a partition is revoked" in assertAllStagesStopped {
      def subSourcesWithProbes(
          partitions: Int,
          probe: TestSubscriber.Probe[(TopicPartition, Source[ConsumerRecord[String, String], NotUsed])]
      ): Seq[(TopicPartition, TestSubscriber.Probe[ConsumerRecord[String, String]])] =
        probe
          .expectNextN(partitions.toLong)
          .map {
            case (tp, subSource) =>
              (tp, subSource.toMat(TestSink.probe)(Keep.right).run())
          }

      def runForSubSource(
          partition: Int,
          subSourcesWithProbes: Seq[(TopicPartition, TestSubscriber.Probe[ConsumerRecord[String, String]])]
      )(fun: TestSubscriber.Probe[ConsumerRecord[String, String]] => Unit) =
        subSourcesWithProbes
          .find { case (tp, _) => tp.partition() == partition }
          .foreach { case (_, probe) => fun(probe) }

      val count = 20L
      // de-coupling consecutive test runs with crossScalaVersions on Travis
      val topicSuffix = Random.nextInt()
      val topic1 = createTopic(topicSuffix, partitions = 2)
      val group1 = createGroupId(1)
      val tp0 = new TopicPartition(topic1, partition0)
      val tp1 = new TopicPartition(topic1, partition1)
      val consumerSettings = consumerDefaults
        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500") // 500 is the default value
        .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[AlpakkaAssignor].getName)
        .withGroupId(group1)

      awaitProduce(produce(topic1, 0 to count.toInt, partition1))

      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(tp0, tp1)
        )
      )

      log.debug("Subscribe to the topic (without demand)")
      val probe1rebalanceActor = TestProbe()
      val probe1subscription = Subscriptions.topics(topic1).withRebalanceListener(probe1rebalanceActor.ref)
      val (control1, probe1) = Consumer
        .plainPartitionedSource(consumerSettings.withClientId(consumerClientId1), probe1subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      log.debug("Await initial partition assignment")
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription,
                                Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      log.debug("read 2 sub sources returned by partitioned source")
      probe1.request(2)
      val probe1RunningSubSourceProbes = subSourcesWithProbes(partitions = 2, probe1)

      log.debug("read one message from probe1 sub source for partition 1")
      probe1RunningSubSourceProbes
        .find { case (tp, _) => tp.partition() == partition1 }
        .foreach { case (_, probe) => probe.requestNext() }

      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(tp0),
          consumerClientId2 -> Set(tp1)
        )
      )

      log.debug("Subscribe to the topic (without demand)")
      val probe2rebalanceActor = TestProbe()
      val probe2subscription = Subscriptions.topics(topic1).withRebalanceListener(probe2rebalanceActor.ref)
      val (control2, probe2) = Consumer
        .plainPartitionedSource(consumerSettings.withClientId(consumerClientId2), probe2subscription)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe2.request(1)
      val probe2RunningSubSourceProbes = subSourcesWithProbes(partitions = 1, probe2)

      log.debug("Await a revoke to consumer 1")
      probe1rebalanceActor.expectMsg(
        TopicPartitionsRevoked(probe1subscription,
                               Set(new TopicPartition(topic1, partition0), new TopicPartition(topic1, partition1)))
      )

      log.debug("the rebalance finishes")
      probe1rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe1subscription, Set(new TopicPartition(topic1, partition0)))
      )
      probe2rebalanceActor.expectMsg(
        TopicPartitionsAssigned(probe2subscription, Set(new TopicPartition(topic1, partition1)))
      )

      log.debug("resume demand on both consumers")
      runForSubSource(partition = 1, probe1RunningSubSourceProbes)(_.request(count))
      runForSubSource(partition = 1, probe2RunningSubSourceProbes)(_.request(count))

      log.debug("no further messages enqueued on probe1 as partition 1 is balanced away")
      runForSubSource(partition = 1, probe1RunningSubSourceProbes)(_.expectComplete())

      val probe2messages = probe2RunningSubSourceProbes
        .find { case (tp, _) => tp.partition() == partition1 }
        .toList
        .flatMap {
          case (_, probe) =>
            probe.expectNextN(count)
        }

      probe2messages should have size count

      probe1.cancel()
      probe2.cancel()

      control1.isShutdown.futureValue shouldBe Done
      control2.isShutdown.futureValue shouldBe Done
    }
  }
}
