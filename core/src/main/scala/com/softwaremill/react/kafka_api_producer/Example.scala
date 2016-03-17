/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka_api_producer

import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

trait Example {
  def producerProvider: () => KafkaProducer[Array[Byte], String] = ???
}

object Example1 extends Example {
  Source(1 to 10000)
    .map(_.toString)
    .map(new ProducerRecord[Array[Byte], String]("topic1", _))
    .to(Producer.sink(producerProvider))
}

object Example2 extends Example {
  Source(1 to 10000)
    .map(_.toString)
    .map(new ProducerRecord[Array[Byte], String]("topic1", _))
    .via(Producer.flow(producerProvider))
    .mapAsync(1)(identity)
    .map { record =>
      println(s"${record.topic}/${record.partition} ${record.value}")
      record
    }
}
