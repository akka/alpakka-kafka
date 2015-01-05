package com.softwaremill.react.kafka

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.consumer.KafkaConsumer
import org.I0Itec.zkclient.ZkClient

import scala.language.implicitConversions


class RichKafkaConsumer(consumer: KafkaConsumer) extends LazyLogging {

  val zkClientFieldName = "kafka$consumer$ZookeeperConsumerConnector$$zkClient"

  def connected() = {
    zkClient().waitUntilConnected(1, TimeUnit.SECONDS)
  }

  def readElems(count: Long, write: (Array[Byte]) => Unit) = {
    var readCount = 0L
    val iterator = consumer.stream.iterator()
    (1L to count).foreach { loopNo =>
      if (readCount < count) {
        // This will block when there are not enough elements
        val msg = iterator.next().message()
        readCount += 1
        write(msg)
      }
      else logger.debug("No more msgs in Kafka stream")
    }
    readCount
  }

  def zkClient() = {
    val field = consumer.connector.getClass.getDeclaredField(zkClientFieldName)
    field.setAccessible(true)
    field.get(consumer.connector).asInstanceOf[ZkClient]
  }
}

object RichKafkaConsumer {
  implicit def consumer2RichConsumer(consumer: KafkaConsumer): RichKafkaConsumer = new RichKafkaConsumer(consumer)
}
