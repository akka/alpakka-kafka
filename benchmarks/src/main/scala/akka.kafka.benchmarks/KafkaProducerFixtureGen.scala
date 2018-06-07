/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.app.RunTestCommand
import org.apache.kafka.clients.producer.KafkaProducer

case class KafkaProducerTestFixture(topic: String, msgCount: Int, producer: KafkaProducer[Array[Byte], String]) {
  def close(): Unit = producer.close()
}

object KafkaProducerFixtures extends PerfFixtureHelpers {

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[KafkaProducerTestFixture](
    c,
    msgCount => {
      KafkaProducerTestFixture("topic", msgCount, null)
    }
  )

  def initializedProducer(c: RunTestCommand) = FixtureGen[KafkaProducerTestFixture](
    c,
    msgCount => {
      val topic = randomId()
      val rawProducer = initTopicAndProducer(c.kafkaHost, topic)
      KafkaProducerTestFixture(topic, msgCount, rawProducer)
    }
  )
}
