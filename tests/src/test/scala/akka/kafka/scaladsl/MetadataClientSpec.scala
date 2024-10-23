/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

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

      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      awaitProduce(produce(topic1, 1 to 10))

      val beginningOffsets = metadataClient
        .getBeginningOffsets(Set(partition0))
        .futureValue

      beginningOffsets(partition0) shouldBe 0

      metadataClient.close()
    }

    "fail in case of an exception during fetch beginning offsets for non-existing topics" in assertAllStagesStopped {
      val group1 = createGroupId(1)
      val nonExistingPartition = new TopicPartition("non-existing topic", 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)

      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      val beginningOffsetsFuture = metadataClient
        .getBeginningOffsets(Set(nonExistingPartition))

      beginningOffsetsFuture.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]

      metadataClient.close()
    }

    "fetch beginning offset for given partition" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)

      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      awaitProduce(produce(topic1, 1 to 10))

      val beginningOffset = metadataClient
        .getBeginningOffsetForPartition(partition0)
        .futureValue

      beginningOffset shouldBe 0

      metadataClient.close()
    }

    "fetch end offsets for given partitions" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      awaitProduce(produce(topic1, 1 to 10))

      val endOffsets = metadataClient.getEndOffsets(Set(partition0)).futureValue

      endOffsets(partition0) shouldBe 10

      metadataClient.close()
    }

    "fail in case of an exception during fetch end offsets for non-existing topics" in assertAllStagesStopped {
      val group1 = createGroupId(1)
      val nonExistingPartition = new TopicPartition("non-existing topic", 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      val endOffsetsFuture = metadataClient.getEndOffsets(Set(nonExistingPartition))

      endOffsetsFuture.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]

      metadataClient.close()
    }

    "fetch end offset for given partition" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      awaitProduce(produce(topic1, 1 to 10))

      val endOffset = metadataClient.getEndOffsetForPartition(partition0).futureValue

      endOffset shouldBe 10

      metadataClient.close()
    }

    "fetch list of topics" in assertAllStagesStopped {
      val group = createGroupId(1)
      val topic1 = createTopic(suffix = 1, partitions = 2)
      val topic2 = createTopic(suffix = 2, partitions = 1)
      val consumerSettings = consumerDefaults.withGroupId(group)
      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      awaitProduce(produce(topic1, 1 to 10, partition = 0))
      awaitProduce(produce(topic1, 1 to 10, partition = 1))
      awaitProduce(produce(topic2, 1 to 10, partition = 0))

      val topics: Map[String, List[PartitionInfo]] = metadataClient.listTopics().futureValue
      val expectedPartitionsForTopic1 = (topic1, 0) :: (topic1, 1) :: Nil
      val expectedPartitionsForTopic2 = (topic2, 0) :: Nil

      topics(topic1).map(mapToTopicPartition) shouldBe expectedPartitionsForTopic1
      topics(topic2).map(mapToTopicPartition) shouldBe expectedPartitionsForTopic2

      metadataClient.close()
    }

    "fetch partitions of given topic" in assertAllStagesStopped {
      val group = createGroupId(1)
      val topic = createTopic(suffix = 1, partitions = 2)
      val consumerSettings = consumerDefaults.withGroupId(group)
      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      awaitProduce(produce(topic, 1 to 10, partition = 0))
      awaitProduce(produce(topic, 1 to 10, partition = 1))

      val partitionsInfo = metadataClient.getPartitionsFor(topic).futureValue

      partitionsInfo.map(_.partition()) shouldBe List(0, 1)

      metadataClient.close()
    }
  }

  private val mapToTopicPartition = (p: PartitionInfo) => (p.topic(), p.partition())
}
