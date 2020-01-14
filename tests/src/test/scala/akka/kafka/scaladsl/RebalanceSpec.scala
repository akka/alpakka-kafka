/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap => CMap}

import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka._
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.RoundRobinAssignor
//import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerPartitionAssignor, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{Set => MSet}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.Random

class RebalanceSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  final val Numbers = (1 to 5000).map(_.toString)
  final val partition1 = 1
  final val consumerClientId1 = "consumer-1"
  final val consumerClientId2 = "consumer-2"
  final val consumerClientId3 = "consumer-3"

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

    "no message loss during partition revocation and re-subscription" in assertAllStagesStopped {
      // BEGIN: vals and defs
      val topicCount = 10
      val partitionCount = 10
      val perPartitionMessageCount = 1000
      val businessSleep = 10L
      val expectedTimeToFinish = 60.seconds // with 19 seconds buffer after last message
      // inmemory message storage along with duplicate message count
      val messageStorage = new CMap[Int, AtomicInteger]().asScala
      val group1 = createGroupId(1)
      val consumerSettings = consumerDefaults
        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20") // 500 is the default value
        //.withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RangeAssignor].getName)
        .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RoundRobinAssignor].getName)
        .withGroupId(group1)
      val topicMap = new CMap[String, MSet[TopicPartition]]().asScala
      val topicIdxMap = new CMap[Int, String]().asScala

      def businessFlow(clientId: String): Flow[CommittableMessage[String, String], CommittableOffset, NotUsed] = {
        Flow.fromFunction { message =>
          val messageVal = message.record.value.toInt
          if (!messageStorage.contains(messageVal)) {
            messageStorage(messageVal) = new AtomicInteger(0)
          }
//          val duplicateCount =
          messageStorage(messageVal).incrementAndGet()
//          if (duplicateCount > 1)
//            log.warn(
//              s"businessFlow offset ${message.committableOffset.partitionOffset.offset} messageId=${messageVal} topicPartition=${message.record.topic}-${message.record.partition} consumerId=${clientId} duplicateCount=${duplicateCount}"
//            )
          // sleep to simulate expensive business logic
          Thread.sleep(businessSleep) // sleep(businessSleep, "business sleep time")
          message.committableOffset
        }
      }

      def subscribeAndConsumeMessages(clientId: String, subscription: AutoSubscription, tpCount: Int, ptCount: Int) =
        Consumer
          .committablePartitionedSource(consumerSettings.withClientId(clientId), subscription)
          .mapAsyncUnordered(tpCount * ptCount) {
            case (topicPartition, topicPartitionStream) => {
              topicPartitionStream
                .via(businessFlow(clientId))
                .map(offsetOption => {
//                  log.debug(
//                    s"topicPartition ${topicPartition.topic}-${offsetOption.partitionOffset.key.partition} offset ${offsetOption.partitionOffset.offset} consumer ${clientId}"
//                  )
                  offsetOption
                })
                .runWith(Committer.sink(committerDefaults))
            }
          }
          .toMat(TestSink.probe)(Keep.both)
          .run()
      // END: vals and defs

      // BEGIN: create topic-partition map
      (1 to topicCount).foreach(topicIdx => {
        val topic1 = createTopic(topicIdx, partitions = partitionCount)
        log.debug(s"created topic topic1=${topic1}")
        topicIdxMap.put(topicIdx, topic1)
        topicMap.put(topic1, CMap.newKeySet[TopicPartition]().asScala)
        (1 to partitionCount).foreach(partitionIdx => {
          val tp1 = new TopicPartition(topic1, partitionIdx - 1)
          topicMap(topic1).add(tp1)
        })
      })
      val topicSet = topicMap.keySet.toSet
      // END: create topic-partition map

      // BEGIN: publish a numeric series tagged messages across all topic-partitions
      val msgTpMap = new CMap[Int, String]().asScala
      val producers: Seq[Future[Done]] = topicIdxMap.keySet
        .map { topicIdx =>
          val topic1 = topicIdxMap(topicIdx)
          val topicOffset = (topicIdx - 1) * partitionCount * perPartitionMessageCount
          (0 until partitionCount).toSet.map { partitionIdx: Int =>
            val startMessageIdx = partitionIdx * perPartitionMessageCount + 1 + topicOffset
            val endMessageIdx = startMessageIdx + perPartitionMessageCount - 1
            val messageRange = startMessageIdx to endMessageIdx
            messageRange.foreach(messageId => {
              val topicPartition = s"${topic1}-${partitionIdx}"
              if (messageId % 1000 == 0)
                log.debug(s"produce messages for topicPartition=${topicPartition} messageId=${messageId}")
              msgTpMap.put(messageId, topicPartition)
            })
            produce(topic1, messageRange, partitionIdx)
          }.toSeq
        }
        .toSeq
        .flatten
      Await.result(Future.sequence(producers), 4.minute)
      // END: publish a numeric series tagged messages across all topic-partitions

      // BEGIN: introduce first consumer1 with all topic-partitions assigned to it
      log.debug(s"BEGIN:1:Subscribe client ${consumerClientId1} to all topic-partitions in parallel")
      val subscription = Subscriptions.topics(topicSet)
      val (control1, probe1) = subscribeAndConsumeMessages(consumerClientId1, subscription, topicCount, partitionCount)
      probe1.ensureSubscription()
      probe1.request(1)
      log.debug(s"END:1:Subscribe client ${consumerClientId1} to all topic-partitions in parallel")

      waitUntilConsumerSummary(group1) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == topicCount * partitionCount
      }
      // END: introduce first consumer1 with all topic-partitions assigned to it

      // BEGIN: introduce second consumer2 with topic-partitions divided between two consumers
      log.debug(s"BEGIN:2:Subscribe client ${consumerClientId2} to all topic-partitions in parallel")
      val (control2, probe2) = subscribeAndConsumeMessages(consumerClientId2, subscription, topicCount, partitionCount)
      probe2.ensureSubscription()
      probe2.request(1)
      log.debug(s"END:2:Subscribe client ${consumerClientId2} to all topic-partitions in parallel")
      waitUntilConsumerSummary(group1) {
        case consumer1 :: consumer2 :: Nil =>
          consumer1.assignment.topicPartitions.size < topicCount * partitionCount && consumer1.assignment.topicPartitions.size + consumer2.assignment.topicPartitions.size == topicCount * partitionCount
      }
      // END: introduce second consumer2 with topic-partitions divided between two consumers

      // BEGIN: introduce third consumer3 with all topic-partitions divided into three consumers
      log.debug(s"BEGIN:3:Subscribe client ${consumerClientId3} to all topic-partitions in parallel")
      val (control3, probe3) = subscribeAndConsumeMessages(consumerClientId3, subscription, topicCount, partitionCount)
      probe3.ensureSubscription()
      //probe2.request(topicCount * partitionCount)
      probe3.request(1)
      log.debug(s"END:3:Subscribe client ${consumerClientId3} to all topic-partitions in parallel")
      waitUntilConsumerSummary(group1) {
        case consumer1 :: consumer2 :: consumer3 :: Nil => true
      }
      // END: introduce third consumer3 with all topic-partitions divided into three consumers

      // BEGIN: cancel consumer1 and assign all topic-partitions to consumer2 and consumer3
      log.debug(s"BEGIN:1:Cancelling client ${consumerClientId1}")
      probe1.cancel()
      control1.shutdown().futureValue shouldBe Done
      log.debug(s"END:1:Cancelling consumer ${consumerClientId1}")
      waitUntilConsumerSummary(group1) {
        case consumer2 :: consumer3 :: Nil => true
      }
      // END: cancel consumer1 and assign all topic-partitions to consumer1 and consumer2

      // BEGIN: cancel consumer2 and assign all topic-partitions to consumer3
      log.debug(s"BEGIN:2:Cancelling consumer ${consumerClientId2}")
      probe2.cancel()
      control2.shutdown().futureValue shouldBe Done
      log.debug(s"END:2:Cancelling consumer ${consumerClientId2}")
      waitUntilConsumerSummary(group1) {
        case consumer3 :: Nil => true
      }
      // END: cancel consumer2 and assign all topic-partitions to consumer3

      // BEGIN: Re-subscribe consumer2
      log.debug(s"BEGIN:4:Re-subscribe client ${consumerClientId2} to all topic-partitions in parallel")
      val (control2b, probe2b) =
        subscribeAndConsumeMessages(consumerClientId2, subscription, topicCount, partitionCount)
      probe2b.ensureSubscription()
      probe2b.request(1)
      log.debug(s"END:4:Re-subscribe client ${consumerClientId2} to all topic-partitions in parallel")
      waitUntilConsumerSummary(group1) {
        case consumer3 :: consumer2b :: Nil => true
      }
      // END: Re-subscribe consumer2

      // BEGIN: cancel consumer3 and assign all topic-partitions to consumer2
      log.debug(s"BEGIN:5:Cancelling consumer ${consumerClientId3}")
      probe3.cancel()
      control3.shutdown().futureValue shouldBe Done
      log.debug(s"END:5:Cancelling consumer ${consumerClientId3}")
      // END: cancel consumer3 and assign all topic-partitions to consumer2

      // BEGIN: let consumer2 consume all remaining messages
      sleep(expectedTimeToFinish, s"sleep to allow consume messages by ${consumerClientId2}")
      // END: let consumer2 consume all remaining messages

      // BEGIN: cancel final consumer2 and wait for shutdown
      log.debug(s"BEGIN:4:Cancelling consumer ${consumerClientId2}")
      probe2b.cancel()
      control2b.shutdown().futureValue shouldBe Done
      log.debug(s"END:4:Cancelling consumer ${consumerClientId2}")
      // END: cancel final consumer2 and wait for shutdown

      // BEGIN: analyze received messages
      val publishedMessageCount = topicCount * partitionCount * perPartitionMessageCount
      log.debug(
        s"handleMessage:: messageStorage keySet.size=${messageStorage.size} publishedMessageCount=${publishedMessageCount}"
      )

      // print a list of replayed messages, a value of 1 means no replay
      import scala.collection.immutable.SortedMap
      val sortedMessageStorage = SortedMap[Int, AtomicInteger]() ++ messageStorage
      val replayed = sortedMessageStorage.filter(_._2.get > 1).map { m =>
        s"  ${m._1} ${msgTpMap(m._1)} replayed count ${m._2.get}"
      }
      log.error(s"Replayed messages:\n${replayed.mkString("\n")}")

      if (messageStorage.size != publishedMessageCount) {
        val s1 = 1 to publishedMessageCount
        val s2 = messageStorage.keySet
        log.error(s"1::missing messages found ${s1.size} != ${s2.size}")
        s1.filter(!s2.contains(_)).foreach(m => log.error(s"missing:1: message ${m} topicPartition ${msgTpMap(m)}"))
      }

      messageStorage.size shouldBe publishedMessageCount

      //fail("uncomment me to dump logs for successful run")
      // END: analyze received messages
    }
  }
}

object AlpakkaAssignor {
  final val clientIdToPartitionMap = new AtomicReference[Map[String, Set[TopicPartition]]]()
}

/**
 * Control the assignment of group members to topic partitions. This requires each consumer to have a distinct
 * client id so that we can filter them during assignment. The member id is a concatenation of the client id and the
 * group member instance id that's generated by the Consumer Group coordinator.
 *
 * Pass a client.id -> Set[TopicPartition] map to `AlpakkaAssignor.clientIdToPartitionMap` **before** you anticipate a
 * rebalance to occur in your test.
 */
class AlpakkaAssignor extends AbstractPartitionAssignor {
  val log: Logger = LoggerFactory.getLogger(getClass)

  override def name(): String = "alpakka-test"

  override def assign(
      partitionsPerTopic: util.Map[String, Integer],
      subscriptions: util.Map[String, ConsumerPartitionAssignor.Subscription]
  ): util.Map[String, util.List[TopicPartition]] = {
    val clientIdToPartitionMap = AlpakkaAssignor.clientIdToPartitionMap.get()

    val mapTps = clientIdToPartitionMap.values.flatten.toSet
    val subscriptionTps = partitionsPerTopic.asScala.flatMap {
      case (topic, partitions) => (0 until partitions).map(p => new TopicPartition(topic, p))
    }.toSet

    val missingFromMap = subscriptionTps.diff(mapTps)

    if (missingFromMap.nonEmpty)
      throw new Exception(
        s"Missing the following partition assignments from the static assignment map: $missingFromMap"
      )

    val assignments = for {
      memberId <- subscriptions.keySet().asScala
    } yield {
      val (_, tps) = clientIdToPartitionMap
        .find { case (clientId, _) => memberId.startsWith(clientId) }
        .getOrElse {
          throw new Exception(s"Couldn't find client id that matches '$memberId' in static assignment map!")
        }
      memberId -> tps.toList.asJava
    }

    log.debug(s"Assignments: $assignments")

    assignments.toMap.asJava
  }
}
