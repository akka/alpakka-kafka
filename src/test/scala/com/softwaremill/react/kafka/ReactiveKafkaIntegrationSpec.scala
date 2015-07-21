package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.ask
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import kafka.producer.ProducerClosedException
import kafka.serializer.{StringDecoder, StringEncoder}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec extends TestKit(ActorSystem("ReactiveKafkaIntegrationSpec"))
    with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  val kafkaHost = "localhost:9092"
  val zkHost = "localhost:2181"

  def uuid() = UUID.randomUUID().toString
  implicit val timeout = Timeout(1 second)
  def defaultWatermarkStrategy = () => WatermarkRequestStrategy(10)
  def parititonizer(in: String): Option[Array[Byte]] = Some(in.hashCode().toString.getBytes)

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
      val publisher = kafka.consume(ConsumerProps(kafkaHost, zkHost, topic, group, new StringDecoder()))(system)
      val kafkaSubscriber = kafka.publish(ProducerProps(kafkaHost, topic, group, encoder))(system)
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
      val publisher = kafka.consume(ConsumerProps(kafkaHost, zkHost, topic, group, new StringDecoder()))
      val producerProps = ProducerProps(kafkaHost, topic, group, encoder)
      val subscriberProps = kafka.producerActorProps(producerProps, requestStrategy = defaultWatermarkStrategy)
      val supervisor = system.actorOf(Props(new TestHelperSupervisor(self, subscriberProps)))
      val kafkaSubscriberActor = TestActorRef(subscriberProps)
        .asInstanceOf[TestActorRef[KafkaActorSubscriber[String]]]
      val kafkaSubscriber = ActorSubscriber[String](kafkaSubscriberActor)
      val subscriberActor = system.actorOf(Props(new ReactiveTestSubscriber))
      val testSubscriber = ActorSubscriber[String](subscriberActor)
      watch(kafkaSubscriberActor)
      publisher.subscribe(testSubscriber)

      // then
        kafkaSubscriberActor.underlyingActor.cleanupResources()
        kafkaSubscriber.onNext("foo")
      expectMsgClass(classOf[Throwable]).getClass should equal(classOf[ProducerClosedException])
    }

    def shouldStartConsuming(fromEnd: Boolean): Unit = {
      // given
      val kafka = newKafka()
      val topic = uuid()
      val group = uuid()
      val encoder = new StringEncoder()
      val input = kafka.publish(ProducerProps(kafkaHost, topic, group, encoder))(system)
      val subscriberActor = system.actorOf(Props(new ReactiveTestSubscriber))
      val output = ActorSubscriber[String](subscriberActor)
      val initialProps = ConsumerProps(kafkaHost, zkHost, topic, group, new StringDecoder())

      // when
      input.onNext("one")
      val props = if (fromEnd)
        initialProps.readFromEndOfStream()
      else
        initialProps
      val inputConsumer = kafka.consume(props)(system)

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
    new ReactiveKafka()
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

class TestHelperSupervisor(parent: ActorRef, props: Props) extends Actor {

  var child: ActorRef = _

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(
    maxNrOfRetries = 10,
    withinTimeRange = 1 minute
  ) {
    case e: Throwable => parent ! e; Stop
  }

  override def preStart(): Unit = {
    super.preStart()
    child = context.actorOf(props)
  }

  override def receive = {
    case _ =>
  }
}