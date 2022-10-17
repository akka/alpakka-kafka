/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.kafka.tests.scaladsl.LogCapturing
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OffsetAggregationSpec extends AnyWordSpec with Matchers with LogCapturing {

  val topicA = "topicA"
  val topicB = "topicB"

  "aggregateOffsets" should {
    "give all offsets for one element" in {
      val in = new TopicPartition(topicA, 1) -> new OffsetAndMetadata(12, OffsetFetchResponse.NO_METADATA)
      KafkaConsumerActor.aggregateOffsets(List(in)) shouldBe Map(in)
    }

    "give the highest offsets" in {
      val in1 = new TopicPartition(topicA, 1) -> new OffsetAndMetadata(42, OffsetFetchResponse.NO_METADATA)
      val in2 = new TopicPartition(topicA, 1) -> new OffsetAndMetadata(12, OffsetFetchResponse.NO_METADATA)
      KafkaConsumerActor.aggregateOffsets(List(in1, in2)) shouldBe Map(in1)
    }

    "give the highest offsets (other order)" in {
      val in1 = new TopicPartition(topicA, 1) -> new OffsetAndMetadata(42, OffsetFetchResponse.NO_METADATA)
      val in2 = new TopicPartition(topicA, 1) -> new OffsetAndMetadata(12, OffsetFetchResponse.NO_METADATA)
      KafkaConsumerActor.aggregateOffsets(List(in2, in1)) shouldBe Map(in1)
    }

    "give the highest offsets (when mixed)" in {
      val in1 = List(
        new TopicPartition(topicA, 1) -> new OffsetAndMetadata(42, OffsetFetchResponse.NO_METADATA),
        new TopicPartition(topicB, 1) -> new OffsetAndMetadata(11, OffsetFetchResponse.NO_METADATA)
      )
      val in2 = List(
        new TopicPartition(topicA, 1) -> new OffsetAndMetadata(12, OffsetFetchResponse.NO_METADATA),
        new TopicPartition(topicB, 1) -> new OffsetAndMetadata(43, OffsetFetchResponse.NO_METADATA)
      )
      KafkaConsumerActor.aggregateOffsets(in1 ++ in2) shouldBe Map(
        new TopicPartition(topicA, 1) -> new OffsetAndMetadata(42, OffsetFetchResponse.NO_METADATA),
        new TopicPartition(topicB, 1) -> new OffsetAndMetadata(43, OffsetFetchResponse.NO_METADATA)
      )
    }
  }

}
