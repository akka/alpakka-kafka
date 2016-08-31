/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.{Message, Result}
import akka.kafka.ProducerSettings
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

object ReactiveKafkaProducerFixtures extends PerfFixtureHelpers {

  val Parallelism = 100

  type K = Array[Byte]
  type V = String
  type In[PassThrough] = Message[K, V, PassThrough]
  type Out[PassThrough] = Result[K, V, PassThrough]
  type FlowType[PassThrough] = Flow[In[PassThrough], Out[PassThrough], NotUsed]

  case class ReactiveKafkaProducerTestFixture[PassThrough](topic: String, msgCount: Int, flow: FlowType[PassThrough])

  private def createProducerSettings(kafkaHost: String)(implicit actorSystem: ActorSystem): ProducerSettings[K, V] =
    ProducerSettings(actorSystem, Some(new ByteArraySerializer), Some(new StringSerializer))
      .withBootstrapServers(kafkaHost)
      .withParallelism(Parallelism)

  def flowFixture(c: RunTestCommand)(implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaProducerTestFixture[Int]](c, msgCount => {
    val flow: FlowType[Int] = Producer.flow(createProducerSettings(c.kafkaHost))
    val topic = randomId()
    initTopicAndProducer(c.kafkaHost, topic)
    ReactiveKafkaProducerTestFixture(topic, msgCount, flow)
  })

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](
    c, msgCount => {
    ReactiveKafkaConsumerTestFixture("topic", msgCount, null)
  }
  )

}
