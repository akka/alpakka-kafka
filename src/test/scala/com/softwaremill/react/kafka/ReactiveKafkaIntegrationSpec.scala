package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.stream.actor.{ActorSubscriberMessage, WatermarkRequestStrategy, ActorSubscriber}
import akka.stream.scaladsl.{PublisherSink, Source}
import akka.testkit.{TestKit, ImplicitSender}
import akka.util.Timeout
import org.reactivestreams.{Publisher, Subscription, Subscriber}
import org.scalatest.{BeforeAndAfterAll, WordSpecLike, Matchers, FlatSpec}

import scala.collection.mutable.ListBuffer
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with
Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ReactiveKafkaIntegrationSpec"))

  val topic = UUID.randomUUID().toString
  val group = "group"
  implicit val timeout = Timeout(1 second)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Reactive kafka streams" must {

    "combine well" in {
      // given
      val kafka = new ReactiveKafka("localhost:9092", "localhost:2181")
      val publisher = kafka.consume(topic, group)(system)
      val kafkaSubscriber = kafka.publish(topic, group)(system)
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
  }
}

class ReactiveTestSubscriber extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  var elements: Vector[String] = Vector.empty

  def receive = {
    case ActorSubscriberMessage.OnNext(element) => elements = elements :+ element.asInstanceOf[String]
    case "get elements" => sender ! elements
  }
}