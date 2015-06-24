package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.ask
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kafka.producer.{KafkaProducer, ProducerClosedException, ProducerProps}
import kafka.serializer.{StringDecoder, StringEncoder}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec
    extends TestKit(ActorSystem(
      "ReactiveKafkaIntegrationSpec",
      ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")
    ))
    with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def uuid() = UUID.randomUUID().toString
  implicit val timeout = Timeout(1 second)

  def parititonizer(in: String): Option[Array[Byte]] = Some(in.hashCode().toInt.toString.getBytes)

  override def afterAll(): Unit = {
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
      val kafkaSubscriber = kafka.publish(topic, group, encoder)(system)
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

    "close producer, kill actor and log error when in trouble" in {
      // given
      val topic = uuid()
      val group = uuid()
      val kafka = newKafka()
      val encoder = new StringEncoder()
      val publisher = kafka.consume(topic, group, new StringDecoder())
      val props = ProducerProps(kafka.host, topic, group)
      val producer = new KafkaProducer(props)
      val supervisor = system.actorOf(Props(new TestHelperSupervisor(self)))
      val subscriberProps = Props(new KafkaActorSubscriber(producer, encoder))
      val kafkaSubscriberActor = TestActorRef(subscriberProps, supervisor, "subscriber")
        .asInstanceOf[TestActorRef[KafkaActorSubscriber[String]]]
      val kafkaSubscriber = ActorSubscriber[String](kafkaSubscriberActor)
      val subscriberActor = system.actorOf(Props(new ReactiveTestSubscriber))
      val testSubscriber = ActorSubscriber[String](subscriberActor)
      watch(kafkaSubscriberActor)
      publisher.subscribe(testSubscriber)

      // then
      EventFilter[ProducerClosedException](message = "producer already closed") intercept {
        kafkaSubscriberActor.underlyingActor.cleanupResources()
        kafkaSubscriber.onNext("foo")
      }
      expectMsgClass(classOf[Throwable]).getClass should equal(classOf[ProducerClosedException])
    }

    def shouldStartConsuming(fromEnd: Boolean): Unit = {
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
        collectedStrings.nonEmpty && collectedStrings.contains("one") == !fromEnd
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

class TestHelperSupervisor(parent: ActorRef) extends Actor {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(
    maxNrOfRetries = 10,
    withinTimeRange = 1 minute
  ) {
    case e: Throwable => parent ! e; Stop
  }

  override def receive = {
    case _ =>
  }
}