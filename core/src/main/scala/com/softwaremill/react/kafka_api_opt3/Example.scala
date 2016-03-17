/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka_api_opt3

import scala.concurrent.Future

import akka.Done
import com.softwaremill.react.kafka_api_producer.Producer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

trait Example {
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  def consumerProvider: () => KafkaConsumer[Array[Byte], String] = ???

  def producerProvider: () => KafkaProducer[Array[Byte], String] = ???

  class DB {
    def save(offset: Long, data: String): Future[Done] = ???
  }
}

// Consume messages and store a representation, including offset, in DB
object Example3 extends Example {
  val db = new DB

  Consumer.source(consumerProvider)
    .mapAsync(1) { msg =>
      db.save(msg.record.offset, msg.record.value).map(_ => msg.transaction.commit())
    }
}

// Connect a Consumer to Provider
object Example4a extends Example {
  Consumer.source(consumerProvider)
    .map(msg => (new ProducerRecord[Array[Byte], String]("topic2", msg.record.value), msg.transaction))
    .to(Producer.sinkWithConsumerTransaction(producerProvider))
}

// Connect a Consumer to Provider
object Example4b extends Example {
  Consumer.source(consumerProvider)
    .map(msg => (new ProducerRecord[Array[Byte], String]("topic2", msg.record.value), msg.transaction))
    .via(Producer.flow2(producerProvider))
    .mapAsync(1)(identity)
    .mapAsync(1)(transaction => transaction.commit)
}
