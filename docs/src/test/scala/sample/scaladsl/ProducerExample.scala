/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.scaladsl

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl._
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import scala.concurrent.Future
import akka.Done

trait ProducerExample {
  val system = ActorSystem("example")

  // #producer
  // #settings
  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
  // #settings
  val kafkaProducer = producerSettings.createKafkaProducer()
  // #producer

  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer.create(system)

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onFailure {
      case e: Throwable =>
        system.log.error(e, e.getMessage)
        system.terminate()
    }
    result.onSuccess { case _ => system.terminate() }
  }
}

object PlainSinkExample extends ProducerExample {
  def main(args: Array[String]): Unit = {
    // #plainSink
    val done = Source(1 to 100)
      .map(_.toString)
      .map { elem =>
        new ProducerRecord[Array[Byte], String]("topic1", elem)
      }
      .runWith(Producer.plainSink(producerSettings))
    // #plainSink

    terminateWhenDone(done)
  }
}

object PlainSinkWithProducerExample extends ProducerExample {
  def main(args: Array[String]): Unit = {
    // #plainSinkWithProducer
    val done = Source(1 to 100)
      .map(_.toString)
      .map { elem =>
        new ProducerRecord[Array[Byte], String]("topic1", elem)
      }
      .runWith(Producer.plainSink(producerSettings, kafkaProducer))
    // #plainSinkWithProducer

    terminateWhenDone(done)
  }
}

object ProducerFlowExample extends ProducerExample {
  def main(args: Array[String]): Unit = {
    // #flow
    val done = Source(1 to 100)
      .map { n =>
        // val partition = math.abs(n) % 2
        val partition = 0
        ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
          "topic1", partition, null, n.toString
        ), n)
      }
      .via(Producer.flow(producerSettings))
      .map { result =>
        val record = result.message.record
        println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value}" +
          s"(${result.message.passThrough})")
        result
      }
      .runWith(Sink.ignore)
    // #flow

    terminateWhenDone(done)
  }
}
