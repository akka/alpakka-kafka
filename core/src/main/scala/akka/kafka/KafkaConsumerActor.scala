/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.actor.{NoSerializationVerificationNeeded, Props}
import akka.kafka.internal.{KafkaConsumerActor => InternalKafkaConsumerActor}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.util.Try

object KafkaConsumerActor {

  /**
   * Message to send for stopping the Kafka consumer actor.
   */
  case object Stop extends NoSerializationVerificationNeeded

  /**
   * Java API:
   * Message to send for stopping the Kafka consumer actor.
   */
  val stop = Stop

  case class StoppingException() extends RuntimeException("Kafka consumer is stopping")

  def props[K, V](settings: ConsumerSettings[K, V]): Props =
    Props(new InternalKafkaConsumerActor(settings)).withDispatcher(settings.dispatcher)

  //metadata fetching
  //NOTE: These block the actor loop!
  object Metadata {
    //requests
    case object ListTopics extends NoSerializationVerificationNeeded
    final case class GetPartitionsFor(topic: String) extends NoSerializationVerificationNeeded
    final case class GetBeginningOffsets(partitions: Set[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class GetEndOffsets(partitions: Set[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class GetOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) extends NoSerializationVerificationNeeded
    final case class GetCommittedOffset(partition: TopicPartition) extends NoSerializationVerificationNeeded
    //responses
    final case class Topics(response: Try[Map[String, List[PartitionInfo]]]) extends NoSerializationVerificationNeeded
    final case class PartitionsFor(response: Try[List[PartitionInfo]]) extends NoSerializationVerificationNeeded
    final case class BeginningOffsets(response: Try[Map[TopicPartition, Long]]) extends NoSerializationVerificationNeeded
    final case class EndOffsets(response: Try[Map[TopicPartition, Long]]) extends NoSerializationVerificationNeeded
    final case class OffsetsForTimes(response: Try[Map[TopicPartition, OffsetAndTimestamp]]) extends NoSerializationVerificationNeeded
    final case class CommittedOffset(response: Try[OffsetAndMetadata]) extends NoSerializationVerificationNeeded
  }
}
