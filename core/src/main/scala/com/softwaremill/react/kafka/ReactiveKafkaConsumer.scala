package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scalaj.collection.Imports._

/**
 * ReactiveKafkaConsumer allows for instantiating KafkaConsumer
 * instances with several combinations of topic, partition, and offset parameters, depending on the
 * use case.
 */
case class ReactiveKafkaConsumer[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: Set[TopicPartition],
    topicPartitionOffsetsMap: Map[TopicPartition, Long],
    consumerProvider: ConsumerProperties[K, V] => Consumer[K, V] = ReactiveKafkaConsumer.defaultInternalConsumerProvider[K, V] _
) {
  val closed: AtomicBoolean = new AtomicBoolean(false)

  lazy val consumer: Consumer[K, V] = {
    val c = consumerProvider(properties)

    if (topicPartitionOffsetsMap.nonEmpty) {
      c.assign(topicPartitionOffsetsMap.keys.toList)
      topicPartitionOffsetsMap.foreach { case (tp, o) => c.seek(tp, o) }
    }
    else if (topicsAndPartitions.nonEmpty) {
      c.assign(topicsAndPartitions.toList)
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

  def defaultInternalConsumerProvider[K, V](
    properties: ConsumerProperties[K, V]
  ): Consumer[K, V] =
    new KafkaConsumer(
      properties.rawProperties,
      properties.keyDeserializer,
      properties.valueDeserializer
    )

  def apply[K, V](properties: ConsumerProperties[K, V]): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, Set(), Map())

  def apply[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: Set[TopicPartition]
  ): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, topicsAndPartitions, Map())

  // Java DSL - topicsAndPartitions
  def apply[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: java.util.Set[TopicPartition]
  ): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, topicsAndPartitions.asScala.toSet)

  // Java DSL - topicPartitionOffsetsMap
  def apply[K, V](
    properties: ConsumerProperties[K, V],
    topicPartitionOffsetsMap: java.util.Map[TopicPartition, java.lang.Long]
  ): ReactiveKafkaConsumer[K, V] =
    ReactiveKafkaConsumer(properties, Set(), topicPartitionOffsetsMap.asScala.toMap)
}