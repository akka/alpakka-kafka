/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka_api_opt1

import scala.concurrent.Future
import akka.Done
import com.softwaremill.react.kafka_api_producer.Producer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import akka.stream.ClosedShape
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.RunnableGraph
import akka.NotUsed
import akka.stream.scaladsl.Broadcast
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Unzip
import akka.stream.scaladsl.Zip

trait Example {
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  def consumerProvider: () => KafkaConsumer[Array[Byte], String] = ???

  def producerProvider: () => KafkaProducer[Array[Byte], String] = ???

  class DB {
    def save(offset: Long, data: String): Future[Done] = ???
  }
}

// Consume messages and store a representation, including offset in DB
object Example3a extends Example {
  val db = new DB

  // this possibly commits the offset before it's saved in db,
  // but that's not a problem for externally stored offset
  // (would also work with auto-commit)
  Consumer.source(consumerProvider)
    .mapAsync(1) { record =>
      db.save(record.offset, record.value)
    }

}

// Consume messages and store a representation, including offset, in DB
object Example3b extends Example {
  val db = new DB

  val save = Flow[ConsumerRecord[Array[Byte], String]]
    .mapAsync(1) { record =>
      db.save(record.offset, record.value).map(_ => record)
    }
  Consumer.process(consumerProvider, save)

}

// Connect a Consumer to Provider
object Example4a extends Example {

  RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val consumer = builder.add(Consumer.flow(consumerProvider))
    val producer = builder.add(Producer.flow(producerProvider).mapAsync(1)(identity))
    val unzip = builder.add(Unzip[ConsumerRecord[Array[Byte], String], ProducerRecord[Array[Byte], String]])
    val zip = builder.add(Zip[ConsumerRecord[Array[Byte], String], ProducerRecord[Array[Byte], String]])

    val f1 = Flow[ConsumerRecord[Array[Byte], String]]
      .map(r => (r, new ProducerRecord[Array[Byte], String]("topic2", r.value)))
    consumer ~> f1 ~> unzip.in
    unzip.out0 ~> zip.in0
    unzip.out1 ~> producer ~> zip.in1
    zip.out.map { case (consumerRecord, _) => consumerRecord }.via(Consumer.record2commit) ~> consumer

    // here we don't take care of the commit confirmations, what should we do with them?

    ClosedShape
  })

}

// Connect a Consumer to Provider
object Example4b extends Example {

  RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val consumer = builder.add(Consumer.flow(consumerProvider))
    val producer = builder.add(Producer.flow2[Array[Byte], String, ConsumerRecord[Array[Byte], String]](producerProvider).mapAsync(1)(identity))

    val f1 = Flow[ConsumerRecord[Array[Byte], String]]
      .map(r => (new ProducerRecord[Array[Byte], String]("topic2", r.value), r))
    consumer ~> f1 ~> producer
    producer.via(Consumer.record2commit) ~> consumer

    // here we don't take care of the commit confirmations, what should we do with them?

    ClosedShape
  })

}
