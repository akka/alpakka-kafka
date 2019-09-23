/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.KafkaConsumerActor
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Await
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

      val beginningOffsetsFuture = MetadataClient
        .getBeginningOffsets(consumerActor, Set(partition0), 1 seconds)
      val beginningOffsets = Await.result(beginningOffsetsFuture, 1 seconds)

      beginningOffsets(partition0) shouldBe 0

      consumerActor ! KafkaConsumerActor.Stop
    }

    "fetch beginning offset for given partition" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      val partition0 = new TopicPartition(topic1, 0)
      val consumerSettings = consumerDefaults.withGroupId(group1)
      val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))

      val beginningOffsetFuture = MetadataClient
        .getBeginningOffsetForPartition(consumerActor, partition0, 1 seconds)
      val beginningOffset = Await.result(beginningOffsetFuture, 1 seconds)

      beginningOffset shouldBe 0

      consumerActor ! KafkaConsumerActor.Stop
    }
  }
}
