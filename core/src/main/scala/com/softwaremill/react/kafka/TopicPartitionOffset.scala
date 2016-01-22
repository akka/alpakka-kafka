package com.softwaremill.react.kafka

/**
 * Used as a parameter to [[ReactiveKafkaConsumer]] when consuming from
 * a specified offset on a designated partition of a Kafka topic.
 *
 * @param topic
 * @param partition
 * @param offset
 */
case class TopicPartitionOffset(topic: String, partition: Int, offset: Long)

object TopicPartitionOffset {
  val NoPartitionSpecified: Int = -1
  val NoOffsetSpecified: Long = -1
}
