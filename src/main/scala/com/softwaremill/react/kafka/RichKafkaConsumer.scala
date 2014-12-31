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

  def readElems(count: Long, write: (Array[Byte]) => Unit) = {
    val iterator = consumer.stream.iterator()
    var readCount = 0L
    (1L to count).foreach { _ =>
      if (iterator.hasNext()) {
        val msg = iterator.next().message()
        readCount += 1
        write(msg)
      }
    }
    readCount
  }
}

object RichKafkaConsumer {
  implicit def consumer2RichConsumer(consumer: KafkaConsumer): RichKafkaConsumer = new RichKafkaConsumer(consumer)
}
