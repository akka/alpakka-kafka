/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.app.RunTestCommand
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.JavaConverters._

case class KafkaConsumerTestFixture(topic: String, msgCount: Int, consumer: KafkaConsumer[Array[Byte], String]) {
  def close(): Unit = consumer.close()
}

object KafkaConsumerFixtures extends PerfFixtureHelpers {

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[KafkaConsumerTestFixture](
    c,
    msgCount => {
      KafkaConsumerTestFixture("topic", msgCount, null)
    }
  )

  def filledTopics(c: RunTestCommand) = FixtureGen[KafkaConsumerTestFixture](
    c,
    msgCount => {
      val topic = randomId()
      fillTopic(c.kafkaHost, topic, msgCount)
      val consumerJavaProps = new java.util.Properties
      consumerJavaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, c.kafkaHost)
      consumerJavaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, randomId())
      consumerJavaProps.put(ConsumerConfig.GROUP_ID_CONFIG, randomId())
      consumerJavaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val consumer =
        new KafkaConsumer[Array[Byte], String](consumerJavaProps, new ByteArrayDeserializer, new StringDeserializer)
      consumer.subscribe(Set(topic).asJava)
      KafkaConsumerTestFixture(topic, msgCount, consumer)
    }
  )
}
