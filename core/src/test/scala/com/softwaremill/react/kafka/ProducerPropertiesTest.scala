/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka

import java.util.UUID

import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.scalatest._

class ProducerPropertiesTest extends WordSpecLike with Matchers {

  def uuid() = UUID.randomUUID().toString
  val brokerList = "localhost:9092"
  val serializer = new StringSerializer()
  val bArrSerializer = new ByteArraySerializer()
  val topic = uuid()
  val clientId = uuid()

  "ProducerProps" must {

    "handle base case" in {

      val config = ProducerProperties(brokerList, topic, serializer, bArrSerializer).rawProperties

      config.getProperty("bootstrap.servers") should be(brokerList)
    }

    "handle async snappy case" in {

      val config = ProducerProperties(brokerList, topic, serializer, bArrSerializer)
        .useSnappyCompression()
        .messageSendMaxRetries(7)
        .requestRequiredAcks(2)
        .clientId("some-client")
        .rawProperties

      config.getProperty("bootstrap.servers") should be(brokerList)
      config.getProperty("compression.type") should be("snappy")
      config.getProperty("retries") should be("7")
      config.getProperty("acks") should be("2")
      config.getProperty("client.id") should be("some-client")
    }
  }

}
