package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
  * {@code ReactiveKafkaConsumer} allows for instantiating {@link org.apache.kafka.clients.consumer.KafkaConsumer}
  * instances with several combinations of topic, partition, and offset parameters, depending on the
  * use case.
  *
  * @param properties
  * @param topicsAndPartitions
  * @param topicPartitionOffset
  * @tparam K
  * @tparam V
  */
case class ReactiveKafkaConsumer[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: Array[TopicPartitionPair] = Array(),
    topicPartitionOffset: TopicPartitionOffsetBase = NullTopicPartitionOffset
) {

  val closed: AtomicBoolean = new AtomicBoolean(false)

  lazy val consumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer(
      properties.rawProperties,
      properties.keyDeserializer,
      properties.valueDeserializer
    )

    if (topicPartitionOffset.isInstanceOf[TopicPartitionOffset]) {
      c.seek(new TopicPartition(topicPartitionOffset.topic, topicPartitionOffset.partition), topicPartitionOffset.offset)
    }
    else if (Option(topicsAndPartitions).isDefined && topicsAndPartitions.length > 0) {
      val topicsPartitions = topicsAndPartitions.map(tp => tp.toTopicPartition).toList
      c.assign(topicsPartitions)
    }
    else {
      val topics = properties.topic.split(",")
      c.subscribe(topics.toList)
    }
    c
  }

  def close() = {
    closed.set(true)
    consumer.wakeup()
  }

}