package com.softwaremill.react.kafka

import PartitionOffset.{NoPartitionSpecified, NoOffsetSpecified}

trait TopicPartitionOffsetBase {
  def topic: String
  def partition: Int
  def offset: Long
}

/**
  * Used as a parameter to {@link ReactiveKafkaConsumer} when consuming from
  * a specified offset on a designated partition of a Kafka topic.
  *
  * @param topic
  * @param partition
  * @param offset
  */
case class TopicPartitionOffset(topic: String, partition: Int, offset: Long) extends TopicPartitionOffsetBase

case object NullTopicPartitionOffset extends TopicPartitionOffsetBase {
  val topic = ""
  val partition = NoPartitionSpecified
  val offset = NoOffsetSpecified
}

object PartitionOffset {
  val NoPartitionSpecified: Int = -1
  val NoOffsetSpecified: Long = -1
}
