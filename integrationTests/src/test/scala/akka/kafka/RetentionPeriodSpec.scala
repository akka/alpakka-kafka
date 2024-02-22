/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import akka.Done
import akka.kafka.scaladsl.{Consumer, SpecBase}
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class RetentionPeriodSpec extends SpecBase with TestcontainersKafkaPerClassLike {
  private final val confluentPlatformVersion = "5.0.0"

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system)
  // The bug commit refreshing circumvents was fixed in Kafka 2.1.0
  // https://issues.apache.org/jira/browse/KAFKA-4682
  // Confluent Platform 5.0.0 bundles Kafka 2.0.0
  // https://docs.confluent.io/current/installation/versions-interoperability.html
    .withKafkaImageTag(confluentPlatformVersion)
    .withZooKeeperImageTag(confluentPlatformVersion)
    .withInternalTopicsReplicationFactor(1)
    .withConfigureKafka { brokerContainers =>
      brokerContainers.foreach {
        _.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
          .withEnv("KAFKA_OFFSETS_RETENTION_MINUTES", "1")
          .withEnv("KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS", "100")
      }
    }

  final val partition1 = 1
  final val consumerClientId1 = "consumer-1"
  final val consumerClientId2 = "consumer-2"
  final val consumerClientId3 = "consumer-3"

  /**
   * This test needs to manually consume events from `__consumer_offsets` without the aid of types from Kafka core
   * https://github.com/akka/alpakka-kafka/issues/1234
   */
//  "While refreshing offsets the consumer" must {
//    "not commit or refresh partitions that are not assigned" in assertAllStagesStopped {
//      val topic1 = createTopic(0, partitions = 2)
//      val group1 = createGroupId(1)
//      val tp0 = new TopicPartition(topic1, partition0)
//      val tp1 = new TopicPartition(topic1, partition1)
//      val consumerSettings = consumerDefaults
//        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
//        .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[AlpakkaAssignor].getName)
//        .withCommitRefreshInterval(1.seconds)
//        .withGroupId(group1)
//
//      awaitProduce(produce(topic1, 0 to 20, partition0))
//
//      AlpakkaAssignor.clientIdToPartitionMap.set(Map(consumerClientId1 -> Set(tp0, tp1)))
//
//      log.debug("Subscribe to the topic (without demand)")
//      val probe1rebalanceActor = TestProbe()
//      val probe1subscription = Subscriptions.topics(topic1).withRebalanceListener(probe1rebalanceActor.ref)
//      val (control1, probe1) = Consumer
//        .committableSource(consumerSettings.withClientId(consumerClientId1), probe1subscription)
//        .toMat(TestSink())(Keep.both)
//        .run()
//
//      log.debug("Await initial partition assignment")
//      probe1rebalanceActor.expectMsg(TopicPartitionsAssigned(probe1subscription, Set(tp0, tp1)))
//
//      log.debug("read one message from probe1 with partition 0")
//      val firstOffset = probe1.requestNext()
//      firstOffset.record.topic() should be(topic1)
//      firstOffset.record.partition() should equal(partition0)
//
//      log.debug("move the partition to the other consumer")
//      AlpakkaAssignor.clientIdToPartitionMap.set(Map(consumerClientId1 -> Set(tp1), consumerClientId2 -> Set(tp0)))
//
//      log.debug("Subscribe to the topic (without demand)")
//      val probe2rebalanceActor = TestProbe()
//      val probe2subscription = Subscriptions.topics(topic1).withRebalanceListener(probe2rebalanceActor.ref)
//      val (control2, probe2) = Consumer
//        .committableSource(consumerSettings.withClientId(consumerClientId2), probe2subscription)
//        .toMat(TestSink())(Keep.both)
//        .run()
//
//      log.debug("Await a revoke to consumer 1")
//      probe1rebalanceActor.expectMsg(TopicPartitionsRevoked(probe1subscription, Set(tp0, tp1)))
//
//      log.debug("the rebalance finishes")
//      probe1rebalanceActor.expectMsg(TopicPartitionsAssigned(probe1subscription, Set(tp1)))
//      probe2rebalanceActor.expectMsg(TopicPartitionsAssigned(probe2subscription, Set(tp0)))
//
//      // this should setup the refreshing offsets on consumer 1 to go backwards
//      log.debug("committing progress on first consumer for {}, after it has been rebalanced away", tp0)
//      firstOffset.committableOffset.commitInternal()
//
//      log.debug("Resume polling on second consumer, committing progress forward for {}", tp0)
//      val offsets = probe2.request(2).expectNextN(2)
//      for (offset <- offsets) {
//        offset.record.topic() should be(topic1)
//        offset.record.partition() should equal(partition0)
//        offset.committableOffset.commitInternal()
//      }
//
//      sleep(5.seconds, "Wait for a number of offset refreshes")
//
//      // read the __consumer_offsets topic and we should see the flipping back and forth
//      val group2 = createGroupId(2)
//      val group2consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] = consumerDefaults(
//        new ByteArrayDeserializer(),
//        new ByteArrayDeserializer()
//      ).withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1").withGroupId(group2)
//      val probe3subscription = Subscriptions.topics("__consumer_offsets")
//      val (control3, probe3) = Consumer
//        .plainSource(group2consumerSettings.withClientId(consumerClientId3), probe3subscription)
//        .toMat(TestSink())(Keep.both)
//        .run()
//      val commits: Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = probe3.request(100).expectNextN(10)
//
//      // helper method ripped from GroupMetadataManager's formatter
//      def readOffset(
//          consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]
//      ): Option[(GroupTopicPartition, Option[OffsetAndMetadata])] = {
//        val offsetKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(consumerRecord.key())) // Only read the message if it is an offset record.
//        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
//        offsetKey match {
//          case offsetKey: OffsetKey =>
//            val groupTopicPartition = offsetKey.key
//            val value = consumerRecord.value
//            val formattedValue =
//              if (value == null) None else Some(GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)))
//            Some((groupTopicPartition, formattedValue))
//              .asInstanceOf[Option[(GroupTopicPartition, Option[OffsetAndMetadata])]]
//          case _: Any => None
//        }
//      }
//
//      log.debug("Ensure that commits progress in-order")
//      var progress = -1L
//      for (consumerRecord <- commits) {
//        readOffset(consumerRecord) match {
//          case Some((group, offset)) =>
//            log.debug("Committed {}: {}", group: Any, offset: Any)
//            offset match {
//              case Some(position) =>
//                if (group.topicPartition.equals(tp0)) {
//                  position.offset shouldBe >=(progress)
//                  progress = position.offset
//                }
//              case None => //noop
//            }
//          case None => // noop
//        }
//      }
//
//      // cleanup
//      probe1.cancel()
//      probe2.cancel()
//      probe3.cancel()
//
//      control1.isShutdown.futureValue shouldBe Done
//      control2.isShutdown.futureValue shouldBe Done
//      control3.isShutdown.futureValue shouldBe Done
//    }
//  }

  "After retention period (1 min) consumer" must {
    "resume from committed offset" in assertAllStagesStopped {
      val topic1 = createTopic()
      val group1 = createGroupId()

      produce(topic1, 1 to 100)

      val committedElements = new ConcurrentLinkedQueue[Int]()

      val consumerSettings = consumerDefaults.withGroupId(group1).withCommitRefreshInterval(5.seconds)

      val (control, probe1) = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .mapAsync(10) { elem =>
          elem.committableOffset.commitInternal().map { _ =>
            committedElements.add(elem.record.value.toInt)
            Done
          }
        }
        .toMat(TestSink())(Keep.both)
        .run()

      probe1
        .request(25)
        .expectNextN(25)
        .toSet should be(Set(Done))

      val longerThanRetentionPeriod = 70.seconds
      sleep(longerThanRetentionPeriod, "Waiting for retention to expire for probe1")

      probe1.cancel()
      Await.result(control.isShutdown, remainingOrDefault)

      val probe2 = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink())

      // Note that due to buffers and mapAsync(10) the committed offset is more
      // than 26, and that is not wrong

      // some concurrent publish
      produce(topic1, 101 to 200)

      val expectedElements = ((committedElements.asScala.max + 1) to 100).map(_.toString)
      probe2
        .request(100)
        .expectNextN(expectedElements)

      sleep(longerThanRetentionPeriod, "Waiting for retention to expire for probe2")

      probe2.cancel()

      val probe3 = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(_.record.value)
        .runWith(TestSink())

      probe3
        .request(100)
        .expectNextN(expectedElements)

      probe3.cancel()
    }
  }
}
