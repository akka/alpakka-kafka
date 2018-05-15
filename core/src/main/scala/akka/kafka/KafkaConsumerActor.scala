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

  /**
   * Kafka metadata fetching support.
   *
   * NOTE: Processing of these requests block the actor loop. The KafkaConsumerActor is configured to run on its
   * own dispatcher, so just as the other remote calls to Kafka, the blocking happens within a designated thread pool.
   * However, calling these during consuming might affect performance and even cause timeouts in extreme cases.
   */
  object Metadata {

    sealed trait Request
    sealed trait Response

    /**
     * [[org.apache.kafka.clients.consumer.KafkaConsumer#listTopics()]]
     */
    case object ListTopics extends Request with NoSerializationVerificationNeeded
    final case class Topics(response: Try[Map[String, List[PartitionInfo]]]) extends Response with NoSerializationVerificationNeeded

    /**
     * [[org.apache.kafka.clients.consumer.KafkaConsumer#partitionsFor()]]
     */
    final case class GetPartitionsFor(topic: String) extends Request with NoSerializationVerificationNeeded
    final case class PartitionsFor(response: Try[List[PartitionInfo]]) extends Response with NoSerializationVerificationNeeded

    /**
     * [[org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets()]]
     *
     * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
     */
    final case class GetBeginningOffsets(partitions: Set[TopicPartition]) extends Request with NoSerializationVerificationNeeded
    final case class BeginningOffsets(response: Try[Map[TopicPartition, Long]]) extends Response with NoSerializationVerificationNeeded

    /**
     * [[org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets()]]
     *
     * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
     */
    final case class GetEndOffsets(partitions: Set[TopicPartition]) extends Request with NoSerializationVerificationNeeded
    final case class EndOffsets(response: Try[Map[TopicPartition, Long]]) extends Response with NoSerializationVerificationNeeded

    /**
     * [[org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes()]]
     *
     * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
     */
    final case class GetOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) extends Request with NoSerializationVerificationNeeded
    final case class OffsetsForTimes(response: Try[Map[TopicPartition, OffsetAndTimestamp]]) extends Response with NoSerializationVerificationNeeded

    /**
     * [[org.apache.kafka.clients.consumer.KafkaConsumer#committed()]]
     */
    final case class GetCommittedOffset(partition: TopicPartition) extends Request with NoSerializationVerificationNeeded
    final case class CommittedOffset(response: Try[OffsetAndMetadata]) extends Response with NoSerializationVerificationNeeded
  }
}
