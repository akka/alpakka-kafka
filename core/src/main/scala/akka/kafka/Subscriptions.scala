/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.actor.ActorRef
import org.apache.kafka.common.TopicPartition

import scala.annotation.varargs
import scala.collection.JavaConverters._

sealed trait Subscription {
  /** ActorRef which is to receive [[akka.kafka.ConsumerRebalanceEvent]] signals when rebalancing happens */
  def rebalanceListener: Option[ActorRef]
  /** Configure this actor ref to receive [[akka.kafka.ConsumerRebalanceEvent]] signals */
  def withRebalanceListener(ref: ActorRef): Subscription
}
sealed trait ManualSubscription extends Subscription {
  def withRebalanceListener(ref: ActorRef): ManualSubscription
}
sealed trait AutoSubscription extends Subscription {
  def withRebalanceListener(ref: ActorRef): AutoSubscription
}

sealed trait ConsumerRebalanceEvent
final case class TopicPartitionsAssigned(sub: Subscription, topicPartitions: Set[TopicPartition]) extends ConsumerRebalanceEvent
final case class TopicPartitionsRevoked(sub: Subscription, topicPartitions: Set[TopicPartition]) extends ConsumerRebalanceEvent

object Subscriptions {
  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class TopicSubscription(tps: Set[String], rebalanceListener: Option[ActorRef]) extends AutoSubscription {
    def withRebalanceListener(ref: ActorRef): TopicSubscription =
      TopicSubscription(tps, Some(ref))
  }
  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class TopicSubscriptionPattern(pattern: String, rebalanceListener: Option[ActorRef]) extends AutoSubscription {
    def withRebalanceListener(ref: ActorRef): TopicSubscriptionPattern =
      TopicSubscriptionPattern(pattern, Some(ref))
  }
  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class Assignment(tps: Set[TopicPartition], rebalanceListener: Option[ActorRef]) extends ManualSubscription {
    def withRebalanceListener(ref: ActorRef): Assignment =
      Assignment(tps, Some(ref))
  }
  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class AssignmentWithOffset(tps: Map[TopicPartition, Long], rebalanceListener: Option[ActorRef]) extends ManualSubscription {
    def withRebalanceListener(ref: ActorRef): AssignmentWithOffset =
      AssignmentWithOffset(tps, Some(ref))
  }
  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class AssignmentOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long], rebalanceListener: Option[ActorRef]) extends ManualSubscription {
    def withRebalanceListener(ref: ActorRef): AssignmentOffsetsForTimes =
      AssignmentOffsetsForTimes(timestampsToSearch, Some(ref))
  }

  /** Creates subscription for given set of topics */
  def topics(ts: Set[String]): AutoSubscription = TopicSubscription(ts, None)

  /**
   * Creates subscription for given set of topics
   * JAVA API
   */
  @varargs
  def topics(ts: String*): AutoSubscription = topics(ts.toSet)

  /**
   * Creates subscription for given set of topics
   * JAVA API
   */
  def topics(ts: java.util.Set[String]): AutoSubscription = topics(ts.asScala.toSet)

  /**
   * Creates subscription for given topics pattern
   */
  def topicPattern(pattern: String): AutoSubscription = TopicSubscriptionPattern(pattern, None)

  /**
   * Manually assign given topics and partitions
   */
  def assignment(tps: Set[TopicPartition]): ManualSubscription = Assignment(tps, None)

  /**
   * Manually assign given topics and partitions
   * JAVA API
   */
  @varargs
  def assignment(tps: TopicPartition*): ManualSubscription = assignment(tps.toSet)

  /**
   * Manually assign given topics and partitions
   * JAVA API
   */
  def assignment(tps: java.util.Set[TopicPartition]): ManualSubscription = assignment(tps.asScala.toSet)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tps: Map[TopicPartition, Long]): ManualSubscription = AssignmentWithOffset(tps, None)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tps: (TopicPartition, Long)*): ManualSubscription = AssignmentWithOffset(tps.toMap, None)

  /**
   * Manually assign given topics and partitions with offsets
   * JAVA API
   */
  def assignmentWithOffset(tps: java.util.Map[TopicPartition, java.lang.Long]): ManualSubscription = assignmentWithOffset(tps.asScala.toMap.asInstanceOf[Map[TopicPartition, Long]])

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tp: TopicPartition, offset: Long): ManualSubscription = assignmentWithOffset(Map(tp -> offset))

  /**
   * Manually assign given topics and partitions with timestamps
   */
  def assignmentOffsetsForTimes(tps: Map[TopicPartition, Long]): ManualSubscription = AssignmentOffsetsForTimes(tps, None)

  /**
   * Manually assign given topics and partitions with timestamps
   */
  def assignmentOffsetsForTimes(tps: (TopicPartition, Long)*): ManualSubscription = AssignmentOffsetsForTimes(tps.toMap, None)

  /**
   * Manually assign given topics and partitions with timestamps
   * JAVA API
   */
  def assignmentOffsetsForTimes(tps: java.util.Map[TopicPartition, java.lang.Long]): ManualSubscription = assignmentOffsetsForTimes(tps.asScala.toMap.asInstanceOf[Map[TopicPartition, Long]])

  /**
   * Manually assign given topics and partitions with timestamps
   */
  def assignmentOffsetsForTimes(tp: TopicPartition, timestamp: Long): ManualSubscription = assignmentOffsetsForTimes(Map(tp -> timestamp))

}
