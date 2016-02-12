package com.softwaremill.react.kafka

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.WatermarkRequestStrategy
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait KafkaTest extends BeforeAndAfterAll {
  this: Suite =>
  implicit def system: ActorSystem
  implicit lazy val materializer = ActorMaterializer()

  case class FixtureParam(topic: String, group: String, kafka: ReactiveKafka)

  def defaultWatermarkStrategy = () => WatermarkRequestStrategy(10)

  val kafkaHost = "localhost:9092"
  val serializer = new StringSerializer()
  val kafka = new ReactiveKafka()

  def createSubscriberProps(kafka: ReactiveKafka, producerProperties: ProducerProperties[String, String]): Props = {
    kafka.producerActorProps(producerProperties, requestStrategy = defaultWatermarkStrategy)
  }

  def createProducerProperties(f: FixtureParam): ProducerProperties[String, String] = {
    ProducerProperties(kafkaHost, f.topic, serializer, serializer)
  }

  def createSource[K, V](f: FixtureParam): Source[ConsumerRecord[String, String], NotUsed] = {
    createSource(f, consumerProperties(f))
  }

  def createSource[K, V](f: FixtureParam, properties: ConsumerProperties[K, V]) = {
    f.kafka.graphStageSource(properties)
  }

  def consumerProperties(f: FixtureParam): ConsumerProperties[String, String] = {
    ConsumerProperties(kafkaHost, f.topic, f.group, new StringDeserializer(), new StringDeserializer()).commitInterval(2 seconds)
  }

  def createTestSubscriber(): ActorRef = {
    system.actorOf(Props(new ReactiveTestSubscriber))
  }

  def stringSubscriber(f: FixtureParam) = {
    f.kafka.publish(ProducerProperties(kafkaHost, f.topic, serializer, serializer))(system)
  }

  def stringGraphSink(f: FixtureParam) = {
    f.kafka.graphStageSink(ProducerProperties(kafkaHost, f.topic, serializer))
  }

  def stringSubscriberActor(f: FixtureParam) = {
    f.kafka.producerActor(ProducerProperties(kafkaHost, f.topic, serializer, serializer))(system)
  }

  def stringConsumer(f: FixtureParam) = {
    f.kafka.consume(consumerProperties(f))(system)
  }

  def stringConsumerWithOffsetSink(f: FixtureParam) = {
    f.kafka.consumeWithOffsetSink(consumerProperties(f))(system)
  }

  def newKafka(): ReactiveKafka = {
    new ReactiveKafka()
  }

  @tailrec
  final def verifyNever(unexpectedCondition: => Boolean, start: Long = System.currentTimeMillis()): Unit = {
    val now = System.currentTimeMillis()
    if (start + 3000 >= now) {
      Thread.sleep(100)
      if (unexpectedCondition)
        fail("Assertion failed before timeout passed")
      else
        verifyNever(unexpectedCondition, start)
    }
  }

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

}
