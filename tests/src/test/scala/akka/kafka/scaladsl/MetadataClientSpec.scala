/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.common.TopicPartition

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

      metadataClient.stop()
    }

    "fail in case of an exception during fetch beginning offsets for non-existing topics" in assertAllStagesStopped {
      val group1 = createGroupId(1)
      val nonExistingPartition = new TopicPartition("non-existing topic", 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)

      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      val beginningOffsetsFuture = metadataClient
        .getBeginningOffsets(Set(nonExistingPartition))

      beginningOffsetsFuture.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]

      metadataClient.stop()
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

      metadataClient.stop()
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

      metadataClient.stop()
    }

    "fail in case of an exception during fetch end offsets for non-existing topics" in assertAllStagesStopped {
      val group1 = createGroupId(1)
      val nonExistingPartition = new TopicPartition("non-existing topic", 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val metadataClient = MetadataClient.create(consumerSettings, 1 second)

      val endOffsetsFuture = metadataClient.getEndOffsets(Set(nonExistingPartition))

      endOffsetsFuture.failed.futureValue shouldBe a[org.apache.kafka.common.errors.InvalidTopicException]

      metadataClient.stop()
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

      metadataClient.stop()
    }
  }
}
