/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka.benchmarks

import com.softwaremill.react.kafka.ProducerProperties
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer

trait QueuePreparations {

  val serializer = new StringSerializer()

  def createProducerProperties(f: Fixture): ProducerProperties[String, String] = {
    ProducerProperties(f.host, f.topic, serializer, serializer)
  }

  def givenQueueWithElements(msgs: Seq[String], f: Fixture) = {
    val prodProps: ProducerProperties[String, String] = createProducerProperties(f)
    val producer = new KafkaProducer(prodProps.rawProperties, prodProps.keySerializer, prodProps.valueSerializer)
    msgs.foreach { msg =>
      producer.send(new ProducerRecord(f.topic, msg))
    }
    producer.close()
  }

}
