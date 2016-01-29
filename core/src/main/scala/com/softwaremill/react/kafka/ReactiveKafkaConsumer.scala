package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scalaj.collection.Imports._

/**
 * ReactiveKafkaConsumer allows for instantiating KafkaConsumer
 * instances with several combinations of topic, partition, and offset parameters, depending on the
 * use case.
 *
 * @param properties
 * @param topicsAndPartitions
 * @param topicPartitionOffsetsMap
 * @tparam K
 * @tparam V
 */
case class ReactiveKafkaConsumer[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: List[TopicPartition],
    topicPartitionOffsetsMap: Map[TopicPartition, Long]
) {

  val closed: AtomicBoolean = new AtomicBoolean(false)

  lazy val consumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer(
      properties.rawProperties,
      properties.keyDeserializer,
      properties.valueDeserializer
    )

    if (topicPartitionOffsetsMap.nonEmpty) {
      c.assign(topicPartitionOffsetsMap.keys.toList)
      topicPartitionOffsetsMap.foreach { case (tp, o) => c.seek(tp, o) }
    }
    else if (topicsAndPartitions.nonEmpty) {
      c.assign(topicsAndPartitions)
    }
    else {
      c.subscribe(List(properties.topic)) // future support for multiple topics?
    }

    c
  }

  def close() = {
    closed.set(true)
    consumer.wakeup()
  }

}

object ReactiveKafkaConsumer {

  def apply[K, V](properties: ConsumerProperties[K, V]): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, List(), Map())

  def apply[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: List[TopicPartition]
  ): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, topicsAndPartitions, Map())

  // Java DSL - topicsAndPartitions
  def apply[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: java.util.List[TopicPartition]
  ): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, topicsAndPartitions.asScala.toList)

  // Java DSL - topicPartitionOffsetsMap
  def apply[K, V](
    properties: ConsumerProperties[K, V],
    topicPartitionOffsetsMap: java.util.Map[TopicPartition, java.lang.Long]
  ): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, List(), topicPartitionOffsetsMap.asScala.toMap)
}