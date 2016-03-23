/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka.scaladsl

import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import akka.NotUsed

trait ProducerExample {
  def producerProvider: () => KafkaProducer[Array[Byte], String] = ???
}

object PlainSinkExample extends ProducerExample {
  Source(1 to 10000)
    .map(_.toString)
    .map(elem => new ProducerRecord[Array[Byte], String]("topic1", elem))
    .to(Producer.plainSink(producerProvider))
}

object ProducerFlowExample extends ProducerExample {
  Source(1 to 10000)
    .map(elem => Producer.Message(new ProducerRecord[Array[Byte], String]("topic1", elem.toString), elem))
    .via(Producer.flow(producerProvider))
    .mapAsync(1)(identity)
    .map { result =>
      val record = result.message.record
      println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value} (${result.message.passThrough}")
      result
    }
}
