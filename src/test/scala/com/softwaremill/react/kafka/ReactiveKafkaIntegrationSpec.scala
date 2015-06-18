package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import kafka.serializer.{StringEncoder, StringDecoder}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike
with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ReactiveKafkaIntegrationSpec"))

  def uuid() = UUID.randomUUID().toString
  implicit val timeout = Timeout(1 second)

  def parititonizer(in: String): Option[Array[Byte]] = Some(in.hashCode().toInt.toString.getBytes)
   
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Reactive kafka streams" must {

    "combine well" in {
      // given
      val topic = uuid()
      val group = uuid()
      val kafka = newKafka()
      val encoder = new StringEncoder()
      val publisher = kafka.consume(topic, group, new StringDecoder())(system)
      val kafkaSubscriber = kafka.publish(topic, group, encoder, parititonizer)(system)
      val subscriberActor = system.actorOf(Props(new ReactiveTestSubscriber))
      val testSubscriber = ActorSubscriber[String](subscriberActor)
      publisher.subscribe(testSubscriber)

      // when
      kafkaSubscriber.onNext("one")
      kafkaSubscriber.onNext("two")

      // then
      awaitCond {
        val collectedStrings = Await.result(subscriberActor ? "get elements", atMost = 1 second)
        collectedStrings == List("one", "two")
      }
    }

    "start consuming from the beginning of stream" in {
      shouldStartConsuming(fromEnd = false)
    }

    "start consuming from the end of stream" in {
      shouldStartConsuming(fromEnd = true)
    }

    def shouldStartConsuming(fromEnd: Boolean) {
      // given
      val kafka = newKafka()
      val topic = uuid()
      val group = uuid()
      val encoder = new StringEncoder()
      val input = kafka.publish(topic, group, encoder)(system)
      val subscriberActor = system.actorOf(Props(new ReactiveTestSubscriber))
      val output = ActorSubscriber[String](subscriberActor)

      // when
      input.onNext("one")
      val inputConsumer = if (fromEnd)
        kafka.consumeFromEnd(topic, group, new StringDecoder())(system)
      else
        kafka.consume(topic, group, new StringDecoder())(system)

      for (i <- 1 to 2000)
        input.onNext("two")
      inputConsumer.subscribe(output)

      // then
      awaitCond {
        val collectedStrings = Await.result(subscriberActor ? "get elements", atMost = 1 second).asInstanceOf[Seq[_]]
        collectedStrings.length > 0 && collectedStrings.contains("one") == !fromEnd
      }
    }
  }

  def newKafka(): ReactiveKafka = {
    new ReactiveKafka("localhost:9092", "localhost:2181")
  }
}

class ReactiveTestSubscriber extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  var elements: Vector[String] = Vector.empty

  def receive = {
    case ActorSubscriberMessage.OnNext(element) =>
      elements = elements :+ element.asInstanceOf[String]
    case "get elements" => sender ! elements
  }
}