/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaProducerBenchmarks extends LazyLogging {

  /**
   * Streams generated numbers to a Kafka producer. Does not commit.
   */
  def plainFlow(fixture: KafkaProducerTestFixture, meter: Meter): Unit = {
    val producer = fixture.producer

    for (i <- 1 to fixture.msgCount) {
      producer.send(new ProducerRecord[Array[Byte], String](fixture.topic, i.toString))
      meter.mark()
    }
    fixture.close()
  }

}
