/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka2

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import scala.collection.JavaConversions._

object ConsumerProvider {
  def apply[K, V](host: String, keyDe: Deserializer[K], valueDe: Deserializer[V]): ConsumerProvider[K, V] = {
    apply(host, keyDe, valueDe, NoSetup)
  }

  def apply[K, V](host: String, keyDe: Deserializer[K], valueDe: Deserializer[V], setup: ConsumerSetup): ConsumerProvider[K, V] = {
    new ConsumerProvider(
      Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> host),
      keyDe,
      valueDe,
      setup
    )
  }
}

case class ConsumerProvider[K, V](
    properties: Map[String, String],
    keyDe: Deserializer[K],
    valueDe: Deserializer[V],
    setup: ConsumerSetup
) extends (() => KafkaConsumer[K, V]) {

  def prop(k: String, v: String) = copy[K, V](properties + (k -> v))
  def props(ps: (String, String)*) = copy[K, V](properties ++ ps.toMap)
  def autoCommit(value: Boolean) = prop("enable.auto.commit", value.toString)
  def groupId(value: String) = prop("group.id", value)
  def setup(setup: ConsumerSetup) = copy[K, V](setup = setup)

  def apply() = {
    val javaProps = properties.foldLeft(new java.util.Properties) {
      case (p, (k, v)) => p.put(k, v); p
    }
    val result = new KafkaConsumer[K, V](javaProps, keyDe, valueDe)
    setup(result)
    result
  }
}

trait ConsumerSetup extends {
  def apply[K, V](consumer: KafkaConsumer[K, V]): Unit
}

object NoSetup extends ConsumerSetup {
  override def apply[K, V](consumer: KafkaConsumer[K, V]): Unit = {}
}

object TopicSubscription {
  def apply(topic: String) = new TopicSubscription(Seq(topic))
}

case class TopicSubscription(topics: Seq[String]) extends ConsumerSetup {
  override def apply[K, V](consumer: KafkaConsumer[K, V]): Unit = {
    consumer.subscribe(topics)
  }
}

case class ManualOffset(offsets: Map[TopicPartition, Long]) extends ConsumerSetup {
  override def apply[K, V](consumer: KafkaConsumer[K, V]): Unit = {
    consumer.assign(offsets.keySet.toList)
    offsets.foreach {
      case (partition, offset) =>
        consumer.seek(partition, offset)
    }
  }
}
