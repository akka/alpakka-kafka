package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicBoolean

import com.softwaremill.react.kafka.ReactiveKafkaConsumer.{NoOffsetSpecified, NoPartitionSpecified}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

case class ReactiveKafkaConsumer[K, V](
    properties: ConsumerProperties[K, V],
    partition: Int = NoPartitionSpecified,
    offset: Long = NoOffsetSpecified
) {

  val closed: AtomicBoolean = new AtomicBoolean(false)

  lazy val consumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer(
      properties.rawProperties,
      properties.keyDeserializer,
      properties.valueDeserializer
    )

    if (partition > NoPartitionSpecified) {
      if (offset > NoOffsetSpecified) {
        c.seek(new TopicPartition(properties.topic, partition), offset)
      }
      else {
        c.assign(List(new TopicPartition(properties.topic, partition)))
      }
    }
    else {
      c.subscribe(List(properties.topic)) // support multiple topics?
    }
    c
  }

  def close() = {
    closed.set(true)
    consumer.wakeup()
  }

}

object ReactiveKafkaConsumer {
  val NoPartitionSpecified: Int = -1
  val NoOffsetSpecified: Long = -1
}
