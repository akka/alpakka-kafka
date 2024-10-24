/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.util.Locale

import akka.kafka.benchmarks.app.RunTestCommand
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.jdk.CollectionConverters._

case class KafkaTransactionTestFixture(sourceTopic: String,
                                       sinkTopic: String,
                                       msgCount: Int,
                                       groupId: String,
                                       consumer: KafkaConsumer[Array[Byte], String],
                                       producer: KafkaProducer[Array[Byte], String]) {
  def close(): Unit = {
    consumer.close()
    producer.close()
  }
}

object KafkaTransactionFixtures extends PerfFixtureHelpers {

  def noopFixtureGen(c: RunTestCommand): FixtureGen[KafkaTransactionTestFixture] =
    FixtureGen[KafkaTransactionTestFixture](c, msgCount => {
      KafkaTransactionTestFixture("sourceTopic", "sinkTopic", msgCount, "groupId", consumer = null, producer = null)
    })

  def initialize(c: RunTestCommand) =
    FixtureGen[KafkaTransactionTestFixture](
      c,
      msgCount => {
        fillTopic(c.filledTopic, c.kafkaHost)
        val groupId = randomId()
        val sinkTopic = randomId()

        val consumerJavaProps = new java.util.Properties
        consumerJavaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, c.kafkaHost)
        consumerJavaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
        consumerJavaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
        consumerJavaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, randomId())
        consumerJavaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        consumerJavaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        consumerJavaProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                              IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH))
        val consumer = new KafkaConsumer[Array[Byte], String](consumerJavaProps)
        consumer.subscribe(Set(c.filledTopic.topic).asJava)

        val producerJavaProps = new java.util.Properties
        producerJavaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
        producerJavaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        producerJavaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, c.kafkaHost)
        producerJavaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true.toString)
        producerJavaProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, randomId())
        val producer = new KafkaProducer[Array[Byte], String](producerJavaProps)

        KafkaTransactionTestFixture(c.filledTopic.topic, sinkTopic, msgCount, groupId, consumer, producer)
      }
    )
}
