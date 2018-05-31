/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import java.util.Optional

import akka.actor.NoSerializationVerificationNeeded
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Messages for Kafka metadata fetching via [[KafkaConsumerActor]].
 *
 * NOTE: Processing of these requests blocks the actor loop. The KafkaConsumerActor is configured to run on its
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
  final case class Topics(response: Try[Map[String, List[PartitionInfo]]]) extends Response with NoSerializationVerificationNeeded {
    /**
     * Java API
     */
    def getResponse: Optional[java.util.Map[String, java.util.List[PartitionInfo]]] =
      response.map { m => Optional.of(m.mapValues(_.asJava).asJava) }.getOrElse(Optional.empty())
  }

  /**
   * Java API:
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#listTopics()]]
   */
  val listTopics: ListTopics.type = ListTopics

  /**
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#partitionsFor()]]
   */
  final case class GetPartitionsFor(topic: String) extends Request with NoSerializationVerificationNeeded
  final case class PartitionsFor(response: Try[List[PartitionInfo]]) extends Response with NoSerializationVerificationNeeded {
    /**
     * Java API
     */
    def getResponse: Optional[java.util.List[PartitionInfo]] = {
      response.map(i => Optional.of(i.asJava)).getOrElse(Optional.empty())
    }
  }

  /**
   * Java API:
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#partitionsFor()]]
   */
  def getPartitionsFor(topic: String): GetPartitionsFor = GetPartitionsFor(topic)

  /**
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets()]]
   *
   * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
   */
  final case class GetBeginningOffsets(partitions: Set[TopicPartition]) extends Request with NoSerializationVerificationNeeded
  final case class BeginningOffsets(response: Try[Map[TopicPartition, Long]]) extends Response with NoSerializationVerificationNeeded {
    /**
     * Java API
     */
    def getResponse: Optional[java.util.Map[TopicPartition, java.lang.Long]] =
      response.map { m => Optional.of(m.mapValues(Long.box).asJava) }.getOrElse(Optional.empty())
  }

  /**
   * Java API:
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets()]]
   *
   * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
   */
  def getBeginningOffsets(partitions: java.util.Set[TopicPartition]): GetBeginningOffsets = GetBeginningOffsets(partitions.asScala.toSet)

  /**
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets()]]
   *
   * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
   */
  final case class GetEndOffsets(partitions: Set[TopicPartition]) extends Request with NoSerializationVerificationNeeded
  final case class EndOffsets(response: Try[Map[TopicPartition, Long]]) extends Response with NoSerializationVerificationNeeded {
    /**
     * Java API
     */
    def getResponse: Optional[java.util.Map[TopicPartition, java.lang.Long]] =
      response.map { m => Optional.of(m.mapValues(Long.box).asJava) }.getOrElse(Optional.empty())
  }

  /**
   * Java API:
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets()]]
   *
   * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
   */
  def getEndOffsets(paritions: java.util.Set[TopicPartition]): GetEndOffsets = GetEndOffsets(paritions.asScala.toSet)

  /**
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes()]]
   *
   * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
   */
  final case class GetOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) extends Request with NoSerializationVerificationNeeded
  final case class OffsetsForTimes(response: Try[Map[TopicPartition, OffsetAndTimestamp]]) extends Response with NoSerializationVerificationNeeded {
    /**
     * Java API
     */
    def getResponse: Optional[java.util.Map[TopicPartition, OffsetAndTimestamp]] =
      response.map { m => Optional.of(m.asJava) }.getOrElse(Optional.empty())
  }

  /**
   * Java API:
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes()]]
   *
   * Warning: KafkaConsumer documentation states that this method may block indefinitely if the partition does not exist.
   */
  def getOffsetForTimes(timestampsToSearch: java.util.Map[TopicPartition, java.lang.Long]): GetOffsetsForTimes =
    GetOffsetsForTimes(timestampsToSearch.asScala.mapValues(_.toLong).toMap)

  /**
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#committed()]]
   */
  final case class GetCommittedOffset(partition: TopicPartition) extends Request with NoSerializationVerificationNeeded
  final case class CommittedOffset(response: Try[OffsetAndMetadata]) extends Response with NoSerializationVerificationNeeded {
    /**
     * Java API
     */
    def getResponse: Optional[OffsetAndMetadata] = Optional.ofNullable(response.toOption.orNull)
  }

  /**
   * Java API:
   * [[org.apache.kafka.clients.consumer.KafkaConsumer#committed()]]
   */
  def getCommitedOffset(partition: TopicPartition): GetCommittedOffset = GetCommittedOffset(partition)
}
