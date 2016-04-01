/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples

import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import akka.kafka.scaladsl.Producer
import akka.kafka.ProducerSettings

trait ProducerExample {
  def producerSettings: ProducerSettings[Array[Byte], String] = ???
}

object PlainSinkExample extends ProducerExample {
  Source(1 to 10000)
    .map(_.toString)
    .map(elem => new ProducerRecord[Array[Byte], String]("topic1", elem))
    .to(Producer.plainSink(producerSettings))
}

object ProducerFlowExample extends ProducerExample {
  Source(1 to 10000)
    .map(elem => Producer.Message(new ProducerRecord[Array[Byte], String]("topic1", elem.toString), elem))
    .via(Producer.flow(producerSettings))
    .map { result =>
      val record = result.message.record
      println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value} (${result.message.passThrough}")
      result
    }
}
