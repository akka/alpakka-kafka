/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.KafkaConsumerActor
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.language.postfixOps
import scala.concurrent.duration._

class MetadataClientSpec extends SpecBase with TestcontainersKafkaLike {

  "MetadataClient" must {
    "fetch beginning offsets for given partitions" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      awaitProduce(produce(topic1, 1 to 10))

      val beginningOffsets = MetadataClient
        .getBeginningOffsets(consumerActor, Set(partition0), 1 seconds)
        .futureValue

      beginningOffsets(partition0) shouldBe 0

      consumerActor ! KafkaConsumerActor.Stop
    }

    "fail in case of an exception during fetch beginning offsets for non-existing topics" in assertAllStagesStopped {
      val group1 = createGroupId(1)
      val nonExistingPartition = new TopicPartition("non-existing topic", 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      val beginningOffsetsFuture = MetadataClient
        .getBeginningOffsets(consumerActor, Set(nonExistingPartition), 1 seconds)

      beginningOffsetsFuture.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]

      consumerActor ! KafkaConsumerActor.Stop
    }

    "fetch beginning offset for given partition" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      awaitProduce(produce(topic1, 1 to 10))

      val beginningOffset = MetadataClient
        .getBeginningOffsetForPartition(consumerActor, partition0, 1 seconds)
        .futureValue

      beginningOffset shouldBe 0

      consumerActor ! KafkaConsumerActor.Stop
    }

    "fetch end offsets for given partitions" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      awaitProduce(produce(topic1, 1 to 10))

      val endOffsets = MetadataClient
        .getEndOffsets(consumerActor, Set(partition0), 1 seconds)
        .futureValue

      endOffsets(partition0) shouldBe 10

      consumerActor ! KafkaConsumerActor.Stop
    }

    "fail in case of an exception during fetch end offsets for non-existing topics" in assertAllStagesStopped {
      val group1 = createGroupId(1)
      val nonExistingPartition = new TopicPartition("non-existing topic", 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      val endOffsetsFuture = MetadataClient
        .getEndOffsets(consumerActor, Set(nonExistingPartition), 1 seconds)

      endOffsetsFuture.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]

      consumerActor ! KafkaConsumerActor.Stop
    }

    "fetch end offset for given partition" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      awaitProduce(produce(topic1, 1 to 10))

      val endOffset = MetadataClient
        .getEndOffsetForPartition(consumerActor, partition0, 1 seconds)
        .futureValue

      endOffset shouldBe 10

      consumerActor ! KafkaConsumerActor.Stop
    }

    "fetch list of topics" in assertAllStagesStopped {
      val group = createGroupId(1)
      val topic1 = createTopic(suffix = 1, partitions = 2)
      val topic2 = createTopic(suffix = 2, partitions = 1)
      val consumerSettings = consumerDefaults.withGroupId(group)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      awaitProduce(produce(topic1, 1 to 10, partition = 0))
      awaitProduce(produce(topic1, 1 to 10, partition = 1))
      awaitProduce(produce(topic2, 1 to 10, partition = 0))

      val topics = MetadataClient
        .getListTopics(consumerActor, 1 second)
        .futureValue

      val expectedPartitionsForTopic1 = (topic1, 0) :: (topic1, 1) :: Nil
      val expectedPartitionsForTopic2 = (topic2, 0) :: Nil

      topics(topic1).leftSideValue.map(mapToTopicPartition) shouldBe expectedPartitionsForTopic1
      topics(topic2).leftSideValue.map(mapToTopicPartition) shouldBe expectedPartitionsForTopic2
    }
  }

  private val mapToTopicPartition = (p: PartitionInfo) => (p.topic(), p.partition())

}
