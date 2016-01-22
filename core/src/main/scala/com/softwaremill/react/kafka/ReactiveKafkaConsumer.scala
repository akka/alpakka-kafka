package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
 * {@code ReactiveKafkaConsumer} allows for instantiating KafkaConsumer
 * instances with several combinations of topic, partition, and offset parameters, depending on the
 * use case.
 *
 * @param properties
 * @param topicsAndPartitions
 * @param topicPartitionOffsets
 * @tparam K
 * @tparam V
 */
case class ReactiveKafkaConsumer[K, V](
    properties: ConsumerProperties[K, V],
    topicsAndPartitions: List[TopicPartition] = List(),
    topicPartitionOffsets: List[TopicPartitionOffset] = List()
) {

  val closed: AtomicBoolean = new AtomicBoolean(false)

  lazy val consumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer(
      properties.rawProperties,
      properties.keyDeserializer,
      properties.valueDeserializer
    )

    if (topicPartitionOffsets.nonEmpty) {
      val topicPartitions = topicPartitionOffsets.map(tp => {
        new TopicPartition(tp.topic, tp.partition)
      })

      c.assign(topicPartitions)

      topicPartitionOffsets.foreach(tpo => c.seek(new TopicPartition(tpo.topic, tpo.partition), tpo.offset))
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