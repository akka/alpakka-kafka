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

  def renderStageAttribute: String

  protected def renderListener: String = ""
}
sealed trait ManualSubscription extends Subscription {

  /** @deprecated Manual subscriptions do never rebalance */
  @deprecated("Manual subscription does never rebalance", "1.0-M2")
  def rebalanceListener: Option[ActorRef] = None

  /** @deprecated Manual subscriptions do never rebalance */
  @deprecated("Manual subscription does never rebalance", "1.0-M2")
  def withRebalanceListener(ref: ActorRef): ManualSubscription
}
sealed trait AutoSubscription extends Subscription {

  /** ActorRef which is to receive [[akka.kafka.ConsumerRebalanceEvent]] signals when rebalancing happens */
  def rebalanceListener: Option[ActorRef]

  /** Configure this actor ref to receive [[akka.kafka.ConsumerRebalanceEvent]] signals */
  def withRebalanceListener(ref: ActorRef): AutoSubscription

  override protected def renderListener: String =
    rebalanceListener match {
      case Some(ref) => s" rebalanceListener $ref"
      case None => ""
    }
}

sealed trait ConsumerRebalanceEvent
final case class TopicPartitionsAssigned(sub: Subscription, topicPartitions: Set[TopicPartition])
    extends ConsumerRebalanceEvent
final case class TopicPartitionsRevoked(sub: Subscription, topicPartitions: Set[TopicPartition])
    extends ConsumerRebalanceEvent

object Subscriptions {

  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class TopicSubscription(tps: Set[String], rebalanceListener: Option[ActorRef])
      extends AutoSubscription {
    def withRebalanceListener(ref: ActorRef): TopicSubscription =
      TopicSubscription(tps, Some(ref))
    def renderStageAttribute: String = s"${tps.mkString(" ")}$renderListener"
  }

  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class TopicSubscriptionPattern(pattern: String, rebalanceListener: Option[ActorRef])
      extends AutoSubscription {
    def withRebalanceListener(ref: ActorRef): TopicSubscriptionPattern =
      TopicSubscriptionPattern(pattern, Some(ref))
    def renderStageAttribute: String = s"pattern $pattern$renderListener"
  }

  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class Assignment(tps: Set[TopicPartition]) extends ManualSubscription {
    def withRebalanceListener(ref: ActorRef): Assignment = this
    def renderStageAttribute: String = s"${tps.mkString(" ")}"
  }

  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class AssignmentWithOffset(tps: Map[TopicPartition, Long]) extends ManualSubscription {
    def withRebalanceListener(ref: ActorRef): AssignmentWithOffset = this
    def renderStageAttribute: String =
      s"${tps.map { case (tp, offset) => s"$tp offset$offset" }.mkString(" ")}"
  }

  /** INTERNAL API */
  @akka.annotation.InternalApi
  private[kafka] final case class AssignmentOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long])
      extends ManualSubscription {
    def withRebalanceListener(ref: ActorRef): AssignmentOffsetsForTimes = this
    def renderStageAttribute: String =
      s"${timestampsToSearch.map { case (tp, timestamp) => s"$tp timestamp$timestamp" }.mkString(" ")}"
  }

  /** Creates subscription for given set of topics */
  def topics(ts: Set[String]): AutoSubscription = TopicSubscription(ts, None)

  /**
   * JAVA API
   * Creates subscription for given set of topics
   */
  @varargs
  def topics(ts: String*): AutoSubscription = topics(ts.toSet)

  /**
   * JAVA API
   * Creates subscription for given set of topics
   */
  def topics(ts: java.util.Set[String]): AutoSubscription = topics(ts.asScala.toSet)

  /**
   * Creates subscription for given topics pattern
   */
  def topicPattern(pattern: String): AutoSubscription = TopicSubscriptionPattern(pattern, None)

  /**
   * Manually assign given topics and partitions
   */
  def assignment(tps: Set[TopicPartition]): ManualSubscription = Assignment(tps)

  /**
   * JAVA API
   * Manually assign given topics and partitions
   */
  @varargs
  def assignment(tps: TopicPartition*): ManualSubscription = assignment(tps.toSet)

  /**
   * JAVA API
   * Manually assign given topics and partitions
   */
  def assignment(tps: java.util.Set[TopicPartition]): ManualSubscription = assignment(tps.asScala.toSet)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tps: Map[TopicPartition, Long]): ManualSubscription = AssignmentWithOffset(tps)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tps: (TopicPartition, Long)*): ManualSubscription = AssignmentWithOffset(tps.toMap)

  /**
   * JAVA API
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tps: java.util.Map[TopicPartition, java.lang.Long]): ManualSubscription =
    assignmentWithOffset(tps.asScala.toMap.asInstanceOf[Map[TopicPartition, Long]])

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tp: TopicPartition, offset: Long): ManualSubscription =
    assignmentWithOffset(Map(tp -> offset))

  /**
   * Manually assign given topics and partitions with timestamps
   */
  def assignmentOffsetsForTimes(tps: Map[TopicPartition, Long]): ManualSubscription =
    AssignmentOffsetsForTimes(tps)

  /**
   * Manually assign given topics and partitions with timestamps
   */
  def assignmentOffsetsForTimes(tps: (TopicPartition, Long)*): ManualSubscription =
    AssignmentOffsetsForTimes(tps.toMap)

  /**
   * JAVA API
   * Manually assign given topics and partitions with timestamps
   */
  def assignmentOffsetsForTimes(tps: java.util.Map[TopicPartition, java.lang.Long]): ManualSubscription =
    assignmentOffsetsForTimes(tps.asScala.toMap.asInstanceOf[Map[TopicPartition, Long]])

  /**
   * Manually assign given topics and partitions with timestamps
   */
  def assignmentOffsetsForTimes(tp: TopicPartition, timestamp: Long): ManualSubscription =
    assignmentOffsetsForTimes(Map(tp -> timestamp))

}
