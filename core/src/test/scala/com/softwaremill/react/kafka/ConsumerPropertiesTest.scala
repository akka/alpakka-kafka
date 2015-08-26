package com.softwaremill.react.kafka

import java.util.UUID

import kafka.serializer.StringDecoder
import org.scalatest._

class ConsumerPropertiesTest extends WordSpecLike with Matchers {

  def uuid() = UUID.randomUUID().toString
  val brokerList = "localhost:9092"
  val zooKeepHost = "localhost:2181"
  val topic = uuid()
  val groupId = uuid()
  val decoder = new StringDecoder()

  "ConsumerProps" must {

    "handle base case" in {

      val config = ConsumerProperties(brokerList, zooKeepHost, topic, groupId, decoder)
        .toConsumerConfig

      config.zkConnect should be(zooKeepHost)
      config.groupId should be(groupId)
      config.clientId should be(groupId)
      config.autoOffsetReset should be("smallest")
      config.offsetsStorage should be("zookeeper")
      config.consumerTimeoutMs should be(1500)
      config.dualCommitEnabled should be(false)
    }

    "handle kafka storage" in {

      val config = ConsumerProperties(brokerList, zooKeepHost, topic, groupId, decoder)
        .readFromEndOfStream()
        .consumerTimeoutMs(1234)
        .kafkaOffsetsStorage(true)
        .toConsumerConfig

      config.zkConnect should be(zooKeepHost)
      config.groupId should be(groupId)
      config.clientId should be(groupId)
      config.autoOffsetReset should be("largest")
      config.offsetsStorage should be("kafka")
      config.dualCommitEnabled should be(true)
      config.consumerTimeoutMs should be(1234)
    }

  }

}