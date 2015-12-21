package com.softwaremill.react.kafka

import java.util.UUID

import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest._

import scala.concurrent.duration._
import scala.language.postfixOps

class ConsumerPropertiesTest extends WordSpecLike with Matchers {

  def uuid() = UUID.randomUUID().toString
  val bootstrapServers = "localhost:9092"
  val topic = uuid()
  val groupId = uuid()
  val deserializer = new StringDeserializer()

  "ConsumerProps" must {

    "handle rich case" in {

      val props = ConsumerProperties(bootstrapServers, topic, groupId, deserializer, deserializer)
        .noAutoCommit()
        .readFromEndOfStream()
        .consumerTimeoutMs(300)
        .commitInterval(2 seconds)
        .rawProperties

      props.getProperty("bootstrap.servers") should be(bootstrapServers)
      props.getProperty("enable.auto.commit") should be("false")
      props.getProperty("auto.offset.reset") should be("latest")
      props.getProperty("consumer.timeout.ms") should be("300")
      props.getProperty("auto.commit.interval.ms") should be("2000")
    }

  }

}