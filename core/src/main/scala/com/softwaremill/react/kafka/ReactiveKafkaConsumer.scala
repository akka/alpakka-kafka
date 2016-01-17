package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConversions._
import ReactiveKafkaConsumer.{NoPartitionSpecified, NoOffsetSpecified}

import scala.collection.mutable.ListBuffer

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

    val topics = properties.topic.split(",").toList

    if (partition > NoPartitionSpecified) {
      if (offset > NoOffsetSpecified) {
        // limit to first topic for consuming from specified offset
        c.seek(new TopicPartition(topics(0), partition), offset)
      }
      else {
        val tps: ListBuffer[TopicPartition] = new ListBuffer[TopicPartition]
        topics.foreach(topic => tps += new TopicPartition(topic, partition))
        c.assign(tps)
      }
    }
    else {
      c.subscribe(topics)
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
