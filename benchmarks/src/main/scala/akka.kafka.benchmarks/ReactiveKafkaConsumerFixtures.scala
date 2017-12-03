/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.internal.ConsumerStage.WrappedConsumerControl
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Source}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.JavaConverters._
import scala.concurrent.Future

case class ReactiveKafkaConsumerTestFixture[T](topic: String, msgCount: Int, source: Source[T, Control])

object ReactiveKafkaConsumerFixtures extends PerfFixtureHelpers {

  private def createConsumerSettings(kafkaHost: String)(implicit actorSystem: ActorSystem) =
    ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaHost)
      .withGroupId(randomId())
      .withClientId(randomId())
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def plainSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](c, msgCount => {
    val topic = randomId()
    fillTopic(c.kafkaHost, topic, msgCount)
    val settings = createConsumerSettings(c.kafkaHost)
    val source = Consumer.plainSource(settings, Subscriptions.topics(topic))
    ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
  })

  def plainSourceSimple(c: RunTestCommand)(implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](c, msgCount => {
    val topic = randomId()
    fillTopic(c.kafkaHost, topic, msgCount)
    val settings = createConsumerSettings(c.kafkaHost)
    val source = Consumer.plainSourceStage(settings, Subscriptions.topics(topic))
    ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
  })

  def committingStage(c: RunTestCommand, batchSize: Int)(implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](c, msgCount => {
    val topic = randomId()
    fillTopic(c.kafkaHost, topic, msgCount)
    val settings = createConsumerSettings(c.kafkaHost)
    val source = Consumer.committingSource(settings, Subscriptions.topics(topic), Flow[ConsumerRecord[Array[Byte], String]].map(identity), batchSize)
    ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
  })

  def commitableSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]](c, msgCount => {
    val topic = randomId()
    fillTopic(c.kafkaHost, topic, msgCount)
    val settings = createConsumerSettings(c.kafkaHost)
    val source = Consumer.committableSource(settings, Subscriptions.topics(topic))
    ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
  })

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](
    c, msgCount => {
    ReactiveKafkaConsumerTestFixture("topic", msgCount, null)
  }
  )

}
