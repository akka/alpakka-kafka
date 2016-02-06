package com.softwaremill.react.kafka2

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Deserializer
import scala.collection.JavaConversions._

object ConsumerProvider {
  def apply[K, V](host: String, keyDe: Deserializer[K], valueDe: Deserializer[V]) = {
    new ConsumerProvider(
      Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> host),
      keyDe,
      valueDe,
      Seq.empty
    )
  }

  def apply[K, V](host: String, topic: String, keyDe: Deserializer[K], valueDe: Deserializer[V]) = {
    new ConsumerProvider(
      Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> host),
      keyDe,
      valueDe,
      Seq(topic)
    )
  }
}

case class ConsumerProvider[K, V](
  properties: Map[String, String],
  keyDe: Deserializer[K],
  valueDe: Deserializer[V],
  topics: Seq[String]
) extends (() => KafkaConsumer[K, V]) {

  def prop(k: String, v: String) = copy[K, V](properties + (k -> v))
  def props(ps: (String, String)*) = copy[K, V](properties ++ ps.toMap)
  def topic(_topic: String) = copy[K, V](topics = topics :+ _topic)
  def topics(_topics: String*) = copy[K, V](topics = topics ++ _topics)
  def autoCommit(value: Boolean) = prop("enable.auto.commit", value.toString)
  def groupId(value: String) = prop("group.id", value)

  def apply() = {
    val javaProps = properties.foldLeft(new java.util.Properties) {
      case (p, (k, v)) => p.put(k, v); p
    }
    val result = new KafkaConsumer[K, V](javaProps, keyDe, valueDe)
    result.subscribe(topics)
    result
  }
}

