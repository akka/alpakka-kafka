/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ProducerMessage.Message
import akka.kafka.benchmarks.ReactiveKafkaProducerFixtures.ReactiveKafkaProducerTestFixture
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, Sink}
import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object ReactiveKafkaProducerBenchmarks extends LazyLogging {
  val streamingTimeout = 3 minutes
  type Fixture = ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]

  /**
   * Iterates over N lazily-generated elements and passes them through a Kafka flow.
   */
  def plainFlow(fixture: ReactiveKafkaProducerTestFixture[Int], meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val future = Source(Stream.from(0, 1))
      .take(fixture.msgCount.toLong)
      .map(number => Message(new ProducerRecord[Array[Byte], String](fixture.topic, number.toString), number))
      .via(fixture.flow)
      .map {
        msg => meter.mark(); msg
      }
      .runWith(Sink.ignore)
    Await.result(future, atMost = streamingTimeout)
    logger.debug("Stream finished")
  }

}
