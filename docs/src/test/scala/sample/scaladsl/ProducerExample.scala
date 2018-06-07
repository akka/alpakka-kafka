/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.scaladsl

import java.util

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.concurrent.Future
import akka.Done
import akka.kafka.ProducerMessage.MultiResultPart

import scala.util.{Failure, Success}

trait ProducerExample {
  val system = ActorSystem("example")

  // #producer
  // #settings
  val config = system.settings.config
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")
  // #settings
  val kafkaProducer = producerSettings.createKafkaProducer()
  // #producer

  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer.create(system)

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}

object PlainSinkExample extends ProducerExample {
  def main(args: Array[String]): Unit = {
    // #plainSink
    val done: Future[Done] =
      Source(1 to 100)
        .map(_.toString)
        .map(value => new ProducerRecord[String, String]("topic1", value))
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
      .map(value => new ProducerRecord[String, String]("topic1", value))
      .runWith(Producer.plainSink(producerSettings, kafkaProducer))
    // #plainSinkWithProducer

    terminateWhenDone(done)
  }
}

object ObserveMetricsExample extends ProducerExample {
  def main(args: Array[String]): Unit = {
    // format:off
    // #producerMetrics
    val metrics: util.Map[org.apache.kafka.common.MetricName, _ <: org.apache.kafka.common.Metric] =
      kafkaProducer.metrics() // observe metrics
    // #producerMetrics
    // format:on
    metrics.clear()
  }
}

object ProducerFlowExample extends ProducerExample {

  def createMessage[KeyType, ValueType, PassThroughType](key: KeyType, value: ValueType, passThrough: PassThroughType) =
    {
      // #singleMessage
      new ProducerMessage.Message[KeyType, ValueType, PassThroughType](
        new ProducerRecord("topicName", key, value),
        passThrough
      )
      // #singleMessage
    }

  def createMultiMessage[KeyType, ValueType, PassThroughType](key: KeyType, value: ValueType, passThrough: PassThroughType) =
    {
      import scala.collection.immutable
      // #multiMessage
      new ProducerMessage.MultiMessage[KeyType, ValueType, PassThroughType](
        immutable.Seq(
          new ProducerRecord("topicName", key, value),
          new ProducerRecord("anotherTopic", key, value)
        ),
        passThrough
      )
      // #multiMessage
    }

  def createPassThroughMessage[KeyType, ValueType, PassThroughType](key: KeyType, value: ValueType, passThrough: PassThroughType) = {
    // format:off
    // #passThroughMessage
    new ProducerMessage.PassThroughMessage(
      passThrough
    )
    // #passThroughMessage
    // format:on
  }

  def main(args: Array[String]): Unit = {
    // format:off
    // #flow
    val done = Source(1 to 100)
      .map { number =>
        val partition = 0
        val value = number.toString
        ProducerMessage.Message(
          new ProducerRecord("topic1", partition, "key", value),
          number
        )
      }

      .via(Producer.flexiFlow(producerSettings))

      .map {
        case ProducerMessage.Result(metadata, message) =>
          val record = message.record
          s"${metadata.topic}/${metadata.partition} ${metadata.offset}: ${record.value}"

        case ProducerMessage.MultiResult(parts, passThrough) =>
          parts.map {
            case MultiResultPart(metadata, record) =>
              s"${metadata.topic}/${metadata.partition} ${metadata.offset}: ${record.value}"
          }.mkString(", ")

        case ProducerMessage.PassThroughResult(passThrough) =>
          s"passed through"
      }
      .runWith(Sink.foreach(println(_)))
    // #flow
    // format:on

    terminateWhenDone(done)
  }
}
