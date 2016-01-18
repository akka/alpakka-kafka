package com.softwaremill.react.kafka

import org.apache.kafka.common.TopicPartition

/**
 * Exists to keep {@link TopicPartition} from Kafka out of our interfaces.
 * @param topic
 * @param partition
 */
case class TopicPartitionPair(topic: String, partition: Int) {

  def toTopicPartition = new TopicPartition(topic, partition)
}
