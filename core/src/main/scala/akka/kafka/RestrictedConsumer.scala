/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.annotation.ApiMayChange
import org.apache.kafka.clients.consumer.{Consumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

/**
 * Offers parts of the [[org.apache.kafka.clients.consumer.Consumer]] API which becomes available to
 * the [[akka.kafka.scaladsl.PartitionAssignmentHandler]] callbacks.
 */
@ApiMayChange
final class RestrictedConsumer(consumer: Consumer[_, _], duration: java.time.Duration) {

  /**
   * See [[org.apache.kafka.clients.consumer.KafkaConsumer#assignment]]
   */
  def assignment(): java.util.Set[TopicPartition] = consumer.assignment()

  /**
   * See [[org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets()]]
   */
  def beginningOffsets(tps: java.util.Collection[TopicPartition]): java.util.Map[TopicPartition, java.lang.Long] =
    consumer.beginningOffsets(tps, duration)

  /**
   * See [[org.apache.kafka.clients.consumer.KafkaConsumer#commitSync(Map, java.time.Duration)]]
   */
  def commitSync(offsets: java.util.Map[TopicPartition, OffsetAndMetadata]): Unit =
    consumer.commitSync(offsets, duration)

  /**
   * See [[org.apache.kafka.clients.consumer.KafkaConsumer#committed(TopicPartition, Duration)]]
   */
  def committed(tp: TopicPartition): OffsetAndMetadata = consumer.committed(tp, duration)

  /**
   * See [[org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets(java.util.Collection[TopicPartition], java.time.Duration)]]
   */
  def endOffsets(tps: java.util.Collection[TopicPartition]): java.util.Map[TopicPartition, java.lang.Long] =
    consumer.endOffsets(tps, duration)

  /**
   * See [[org.apache.kafka.clients.consumer.KafkaConsumer#position(TopicPartition, java.time.Duration)]]
   */
  def position(tp: TopicPartition): Long = consumer.position(tp, duration)

  /**
   * See [[org.apache.kafka.clients.consumer.KafkaConsumer#seek(TopicPartition, Long)]]
   */
  def seek(tp: TopicPartition, offset: Long): Unit = consumer.seek(tp, offset)
}
