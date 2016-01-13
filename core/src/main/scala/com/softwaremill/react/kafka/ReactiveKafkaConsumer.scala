package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._

case class ReactiveKafkaConsumer[K, V](properties: ConsumerProperties[K, V]) {

  val closed: AtomicBoolean = new AtomicBoolean(false)

  lazy val consumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer(
      properties.rawProperties,
      properties.keyDeserializer,
      properties.valueDeserializer
    )
    c.subscribe(List(properties.topic)) // support multiple topics?
    c
  }

  def close() = {
    closed.set(true)
    consumer.wakeup()
  }

}
