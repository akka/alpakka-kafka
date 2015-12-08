package com.softwaremill.react.kafka

import java.util.UUID

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest._

class ConsumerPropertiesTest extends WordSpecLike with Matchers {

  def uuid() = UUID.randomUUID().toString
  val bootstrapServers = "localhost:9092"
  val topic = uuid()
  val groupId = uuid()
  val deserializer = new StringDeserializer()

  "ConsumerProps" must {

    "handle base case" in {

      val props = ConsumerProperties(bootstrapServers, topic, groupId, deserializer, deserializer)
        .rawProperties

      //      props.getProperty() should be(zooKeepHost)
      //      config.groupId should be(groupId)
      //      config.clientId should be(groupId)
      //      config.autoOffsetReset should be("smallest")
      //      config.offsetsStorage should be("zookeeper")
      //      config.consumerTimeoutMs should be(1500)
      //      config.dualCommitEnabled should be(false)
    }

    "handle kafka storage" in {

      val config = ConsumerProperties(bootstrapServers, topic, groupId, deserializer, deserializer)
        .readFromEndOfStream()
        .consumerTimeoutMs(1234)
        .rawProperties

      //      config.zkConnect should be(zooKeepHost)
      //      config.groupId should be(groupId)
      //      config.clientId should be(groupId)
      //      config.autoOffsetReset should be("largest")
      //      config.offsetsStorage should be("kafka")
      //      config.dualCommitEnabled should be(true)
      //      config.consumerTimeoutMs should be(1234)
    }

  }

}