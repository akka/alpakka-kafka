package com.softwaremill.react.kafka2

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.Serializer

object ProducerProvider {
  def apply[K, V](host: String, keySe: Serializer[K], valueSe: Serializer[V]) = {
    new ProducerProvider(
      Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> host),
      keySe,
      valueSe
    )
  }
}

case class ProducerProvider[K, V](
  properties: Map[String, String],
  keySe: Serializer[K],
  valueSe: Serializer[V]
) extends (() => KafkaProducer[K, V]) {

  def prop(k: String, v: String) = copy[K, V](properties + (k -> v))
  def props(ps: (String, String)*) = copy[K, V](properties ++ ps.toMap)
  def apply() = {
    val javaProps = properties.foldLeft(new java.util.Properties) {
      case (p, (k, v)) => p.put(k, v); p
    }
    new KafkaProducer[K, V](javaProps, keySe, valueSe)
  }
}

