/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.kafka.benchmarks

import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import scala.concurrent.duration._

object KafkaProducerBenchmarks extends LazyLogging {

  val logStep = 100000

  /**
   * Streams generated numbers to a Kafka producer. Does not commit.
   */
  def plainFlow(fixture: KafkaProducerTestFixture, meter: Meter): Unit = {
    val producer = fixture.producer
    var lastPartStart = System.nanoTime()

    val msg = PerfFixtureHelpers.stringOfSize(fixture.msgSize)

    for (i <- 1 to fixture.msgCount) {
      val partition: Int = (i % fixture.numberOfPartitions).toInt
      producer.send(
        new ProducerRecord[Array[Byte], String](fixture.topic, partition, null, msg),
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = meter.mark()
        }
      )

      if (i % logStep == 0) {
        val lastPartEnd = System.nanoTime()
        val took = (lastPartEnd - lastPartStart).nanos
        logger.info(s"Sent $i, took ${took.toMillis} ms to send last $logStep")
        lastPartStart = lastPartEnd
      }
    }
    fixture.close()
  }

}
