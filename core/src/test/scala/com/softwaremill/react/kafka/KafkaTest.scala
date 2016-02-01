package com.softwaremill.react.kafka

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.actor.WatermarkRequestStrategy
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.kafka.clients.consumer.ConsumerRecord
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
  val zkHost = "localhost:2181"

  val kafka = new ReactiveKafka()

  def createSubscriberProps(kafka: ReactiveKafka, producerProperties: ProducerProperties[String]): Props = {
    kafka.producerActorProps(producerProperties, requestStrategy = defaultWatermarkStrategy)
  }

  def createProducerProperties(f: FixtureParam): ProducerProperties[String] = {
    ProducerProperties(kafkaHost, f.topic, f.group, new StringEncoder())
  }

  def consumerProperties(f: FixtureParam): ConsumerProperties[String] = {
    ConsumerProperties(kafkaHost, zkHost, f.topic, f.group, new StringDecoder()).commitInterval(2 seconds)
  }

  def createSource(f: FixtureParam): Source[KafkaMessage[String], Unit] = {
    createSource(f, consumerProperties(f))
  }

  def createSource[V](f: FixtureParam, properties: ConsumerProperties[V]) = {
    f.kafka.graphStageSource(properties)
  }

  def createTestSubscriber(): ActorRef = {
    system.actorOf(Props(new ReactiveTestSubscriber))
  }

  def stringSubscriber(f: FixtureParam) = {
    val encoder = new StringEncoder()
    f.kafka.publish(ProducerProperties(kafkaHost, f.topic, encoder))(system)
  }

  def stringSubscriberActor(f: FixtureParam) = {
    val encoder = new StringEncoder()
    f.kafka.producerActor(ProducerProperties(kafkaHost, f.topic, encoder))(system)
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
