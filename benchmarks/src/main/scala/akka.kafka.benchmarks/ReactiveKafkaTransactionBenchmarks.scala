/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.ProducerMessage
import akka.kafka.benchmarks.ReactiveKafkaTransactionFixtures.{KProducerMessage, KResult, KTransactionMessage}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps
import scala.util.Success

object ReactiveKafkaTransactionBenchmarks extends LazyLogging {
  val streamingTimeout: FiniteDuration = 30 minutes
  type TransactionFixture = ReactiveKafkaTransactionTestFixture[KTransactionMessage, KProducerMessage, KResult]

  /**
   *
   */
  def consumeTransformProduceTransaction(fixture: TransactionFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val msgCount = fixture.msgCount
    val sinkTopic = fixture.sinkTopic
    val source = fixture.source

    val promise = Promise[Unit]
    val logPercentStep = 1
    val loggedStep = if (msgCount > logPercentStep) 100 else 1

    val control = source
      .map { msg =>
        ProducerMessage.Message(
          new ProducerRecord[Array[Byte], String](sinkTopic, msg.record.value()), msg.partitionOffset)
      }
      .via(fixture.flow)
      .toMat(
        Sink.foreach { result =>
          val offset = result.offset
          if (result.offset % loggedStep == 0)
            logger.info(s"Transformed $offset elements to Kafka (${100 * offset / msgCount}%)")
          if (result.offset >= fixture.msgCount - 1)
            promise.complete(Success(()))
        })(Keep.left)
      .run()

    Await.result(promise.future, streamingTimeout)
    control.shutdown()
    logger.debug("Stream finished")
  }
}
