package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.ask
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import akka.testkit.{EventFilter, ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessage._
import com.typesafe.config.ConfigFactory
import kafka.producer.ProducerClosedException
import kafka.serializer.{StringDecoder, StringEncoder}
import org.scalatest.fixture.WordSpecLike
import org.scalatest.{BeforeAndAfterAll, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec
    extends TestKit(ActorSystem(
      "ReactiveKafkaIntegrationSpec",
      ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")
    ))
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

  def withFixture(test: OneArgTest) = {
    val topic = uuid()
    val group = uuid()
    val kafka = newKafka()
    val theFixture = FixtureParam(topic, group, kafka)
    withFixture(test.toNoArgTest(theFixture))
  }

  case class FixtureParam(topic: String, group: String, kafka: ReactiveKafka)

  "Reactive kafka streams" must {
    "combine well" in { f =>
      // given
      val publisher = stringConsumer(f)
      val kafkaSubscriber = stringSubscriber(f)
      val subscriberActor = createTestSubscriber()
      val testSubscriber = ActorSubscriber[StringKafkaMessage](subscriberActor)
      publisher.subscribe(testSubscriber)

      // when
      kafkaSubscriber.onNext("one")
      kafkaSubscriber.onNext("two")

      // then
      awaitCond {
        val collectedStrings = Await.result(subscriberActor ? "get elements", atMost = 1 second)
          .asInstanceOf[Seq[StringKafkaMessage]]
        collectedStrings.map(_.msg) == List("one", "two")
      }
    }

    "start consuming from the beginning of stream" in { f =>
      shouldStartConsuming(fromEnd = false, f)
    }

    "start consuming from the end of stream" in { f =>
      shouldStartConsuming(fromEnd = true, f)
    }

    "close producer, kill actor and log error when in trouble" in { f =>
      // given
      val publisher = f.kafka.consume(consumerProperties(f))
      val producerProperties = createProducerProperties(f)
      val subscriberProps = createSubscriberProps(f.kafka, producerProperties)
      val supervisor = system.actorOf(Props(new TestHelperSupervisor(self, subscriberProps)))
      val kafkaSubscriberActor = Await.result(supervisor ? "supervise!", atMost = 1 second).asInstanceOf[ActorRef]
      val kafkaSubscriber = ActorSubscriber[String](kafkaSubscriberActor)
      val subscriberActor = createTestSubscriber()
      val testSubscriber = ActorSubscriber[StringKafkaMessage](subscriberActor)
      publisher.subscribe(testSubscriber)

      // when
      EventFilter[ProducerClosedException](message = "producer already closed") intercept {
        kafkaSubscriberActor ! "Stop"
        kafkaSubscriber.onNext("foo")
      }
      // then
      expectMsgClass(classOf[Throwable]).getClass should equal(classOf[ProducerClosedException])
    }

    def shouldStartConsuming(fromEnd: Boolean, f: FixtureParam): Unit = {
      // given
      val input = stringSubscriber(f)
      val subscriberActor = createTestSubscriber()
      val output = ActorSubscriber[StringKafkaMessage](subscriberActor)
      val initialProps = consumerProperties(f)

      // when
      input.onNext("one")
      waitUntilFirstElementInQueue()

      val props = if (fromEnd)
        initialProps.readFromEndOfStream()
      else
        initialProps
      val inputConsumer = f.kafka.consume(props)(system)

      for (i <- 1 to 2000)
        input.onNext("two")
      inputConsumer.subscribe(output)

      // then
      awaitCond {
        val collectedStrings = Await.result(subscriberActor ? "get elements", atMost = 1 second)
          .asInstanceOf[Seq[StringKafkaMessage]]
        collectedStrings.nonEmpty && collectedStrings.map(_.msg).contains("one") == !fromEnd
      }
    }
  }

  def waitUntilFirstElementInQueue(): Unit = {
    // Can't think of any sensible solution at the moment
    Thread.sleep(5000)
  }

  def createSubscriberProps(kafka: ReactiveKafka, producerProperties: ProducerProperties[String]): Props = {
    kafka.producerActorProps(producerProperties, requestStrategy = defaultWatermarkStrategy)
  }

  def createProducerProperties(f: FixtureParam): ProducerProperties[String] = {
    ProducerProperties(kafkaHost, f.topic, f.group, new StringEncoder())
  }

  def consumerProperties(f: FixtureParam): ConsumerProperties[String] = {
    ConsumerProperties(kafkaHost, zkHost, f.topic, f.group, new StringDecoder())
  }

  def createTestSubscriber(): ActorRef = {
    system.actorOf(Props(new ReactiveTestSubscriber))
  }

  def stringSubscriber(f: FixtureParam) = {
    val encoder = new StringEncoder()
    f.kafka.publish(ProducerProperties(kafkaHost, f.topic, f.group, encoder))(system)
  }

  def stringConsumer(f: FixtureParam) = {
    f.kafka.consume(consumerProperties(f))(system)
  }

  def newKafka(): ReactiveKafka = {
    new ReactiveKafka()
  }
}

class ReactiveTestSubscriber extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  var elements: Vector[StringKafkaMessage] = Vector.empty

  def receive = {

    case ActorSubscriberMessage.OnNext(element) =>
      elements = elements :+ element.asInstanceOf[StringKafkaMessage]
    case "get elements" => sender ! elements
  }
}

class TestHelperSupervisor(parent: ActorRef, props: Props) extends Actor {

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(
    maxNrOfRetries = 10,
    withinTimeRange = 1 minute
  ) {
    case e =>
      parent ! e
      Stop
  }

  override def receive = {
    case _ => sender ! context.actorOf(props)
  }
}