/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
case class ReactiveKafkaConsumerTestFixture[T](topic: String, msgCount: Int, source: Source[T, Control])

object ReactiveKafkaConsumerFixtures extends PerfFixtureHelpers {

  private def createConsumerSettings(kafkaHost: String)(implicit actorSystem: ActorSystem) =
    ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaHost)
      .withGroupId(randomId())
      .withClientId(randomId())
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def plainSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) =
    FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](
      c,
      msgCount => {
        val topic = randomId()
        fillTopic(c.kafkaHost, topic, msgCount)
        val settings = createConsumerSettings(c.kafkaHost)
        val source = Consumer.plainSource(settings, Subscriptions.topics(topic))
        ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
      }
    )

  def committableSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) =
    FixtureGen[ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]](
      c,
      msgCount => {
        val topic = randomId()
        fillTopic(c.kafkaHost, topic, msgCount)
        val settings = createConsumerSettings(c.kafkaHost)
        val source = Consumer.committableSource(settings, Subscriptions.topics(topic))
        ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
      }
    )

  def noopFixtureGen(c: RunTestCommand) =
    FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](
      c,
      msgCount => {
        ReactiveKafkaConsumerTestFixture("topic", msgCount, null)
      }
    )

}
