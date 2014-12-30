package com.softwaremill.react.kafka

import java.util.concurrent.TimeUnit

import kafka.consumer.KafkaConsumer
import org.I0Itec.zkclient.ZkClient

import scala.language.implicitConversions

class RichKafkaConsumer(consumer: KafkaConsumer) {
  def connected() = {
    consumer.connector.getClass.getField("zkClient").setAccessible(true)
    val zkClient = consumer.connector.getClass.getField("zkClient").get(consumer.connector).asInstanceOf[ZkClient]
    zkClient.waitUntilConnected(1, TimeUnit.MILLISECONDS)
  }
}

object RichKafkaConsumer {
  implicit def consumer2RichConsumer(consumer: KafkaConsumer): RichKafkaConsumer = new RichKafkaConsumer(consumer)
}
