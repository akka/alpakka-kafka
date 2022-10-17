/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.net.URLEncoder

import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka.{Subscription, Subscriptions}
import akka.util.ByteString
import org.apache.kafka.common.TopicPartition
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionsSpec extends AnyWordSpec with Matchers with LogCapturing {

  "URL encoded subscription" should {
    "be readable for topics" in {
      encode(Subscriptions.topics(Set("topic1", "topic2"))) should be(
        "topic1+topic2"
      )
    }

    "be readable for patterns" in {
      encode(Subscriptions.topicPattern("topic.*")) should be("pattern+topic.*")
    }

    "be readable for assignments" in {
      encode(Subscriptions.assignment(Set(new TopicPartition("topic1", 1)))) should be("topic1-1")
    }

    "be readable for assignments with offset" in {
      encode(Subscriptions.assignmentWithOffset(Map(new TopicPartition("topic1", 1) -> 123L))) should be(
        "topic1-1+offset123"
      )
    }

    "be readable for multiple assignments with offset" in {
      encode(
        Subscriptions.assignmentWithOffset(
          Map(new TopicPartition("topic1", 1) -> 123L, new TopicPartition("A-Topic-Name", 2) -> 456L)
        )
      ) should be(
        "topic1-1+offset123+A-Topic-Name-2+offset456"
      )
    }

    "be readable for multiple assignments with timestamp" in {
      encode(
        Subscriptions.assignmentOffsetsForTimes(
          Map(new TopicPartition("topic1", 1) -> 12345L, new TopicPartition("Another0Topic", 1) -> 998822L)
        )
      ) should be(
        "topic1-1+timestamp12345+Another0Topic-1+timestamp998822"
      )
    }
  }

  private def encode(subscription: Subscription) =
    URLEncoder.encode(subscription.renderStageAttribute, ByteString.UTF_8)

}
