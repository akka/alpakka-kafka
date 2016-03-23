/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka.scaladsl

import scala.concurrent.Future

import akka.Done
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

trait ConsumerExample {
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  def consumerProvider: () => KafkaConsumer[Array[Byte], String] = ???

  def producerProvider: () => KafkaProducer[Array[Byte], String] = ???

  class DB {
    def save(offset: Consumer.Offset, data: String): Future[Done] = ???

    def update(data: String): Future[Done] = ???
  }

  class Rocket {
    def launch(destination: String): Future[Done] = ???
  }
}

// Consume messages and store a representation, including offset, in DB
object ExternalOffsetStorageExample extends ConsumerExample {
  val db = new DB

  Consumer.plainSource(consumerProvider)
    .mapAsync(1) { msg =>
      db.save(msg.offset, msg.value)
    }
}

// Consume messages at-most-once
object AtMostOnceExample extends ConsumerExample {
  val rocket = new Rocket

  Consumer.atMostOnceSource(consumerProvider)
    .mapAsync(1) { record =>
      rocket.launch(record.value)
    }
}

// Consume messages at-least-once
object AtLeastOnceExample extends ConsumerExample {
  val db = new DB

  Consumer.committableSource(consumerProvider)
    .mapAsync(1) { msg =>
      db.update(msg.value).map(_ => msg.committableOffset.commit())
    }
}

// Connect a Consumer to Producer
object ConsumerToProducerSinkExample extends ConsumerExample {
  Consumer.committableSource(consumerProvider)
    .map(msg =>
      Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .to(Producer.commitableOffsetSink(producerProvider))
}

// Connect a Consumer to Producer
object ConsumerToProducerFlowExample extends ConsumerExample {
  Consumer.committableSource(consumerProvider)
    .map(msg =>
      Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .via(Producer.flow(producerProvider))
    .mapAsync(10)(identity)
    .mapAsync(1) { result => result.message.passThrough.commit() }
}

