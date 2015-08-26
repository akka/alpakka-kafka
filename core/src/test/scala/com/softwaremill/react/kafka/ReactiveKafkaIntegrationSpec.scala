package com.softwaremill.react.kafka

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.ask
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{EventFilter, ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.commit.CommitSink
import com.typesafe.config.ConfigFactory
import kafka.producer.ProducerClosedException
import org.scalatest.{Matchers, fixture}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec
    extends TestKit(ActorSystem(
      "ReactiveKafkaIntegrationSpec",
      ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")
    ))
    with ImplicitSender with fixture.WordSpecLike with Matchers with ReactiveKafkaIntegrationTestSupport {

  implicit val timeout = Timeout(1 second)

  def parititonizer(in: String): Option[Array[Byte]] = Some(in.hashCode().toString.getBytes)

  "Reactive kafka streams" must {
    "publish and consume" in { implicit f =>
      // given
      givenQueueWithElements(Seq("a", "b", "c"))

      // then
      verifyQueueHas(Seq("a", "b", "c"))
    }

    "manually commit offsets with kafka" in { implicit f =>
      shouldCommitOffsets("kafka")
    }

    def shouldCommitOffsets(storage: String)(implicit f: FixtureParam) = {
      // given
      givenQueueWithElements(Seq("0", "1", "2", "3", "4", "5"), storage)

      // when
      val consumerProps = consumerProperties(f)
        .commitInterval(100 millis)
        .noAutoCommit()
        .setProperty("offsets.storage", storage)

      val actorWithConsumer = f.kafka.consumerActorWithConsumer(consumerProps, ReactiveKafka.ConsumerDefaultDispatcher)
      val publisherWithCommitSink = PublisherWithCommitSink[String](
        ActorPublisher[StringKafkaMessage](actorWithConsumer.actor),
        CommitSink.create(actorWithConsumer.consumer)
      )
      Source(publisherWithCommitSink.publisher)
        .filter(_.message().toInt < 3)
        .to(publisherWithCommitSink.offsetCommitSink).run()
      Thread.sleep(3000) // how to wait for commit?
      cancelConsumer(actorWithConsumer.actor)

      // then
      verifyQueueHas(Seq("3", "4", "5"), storage)
    }

    "start consuming from the beginning of stream" in { f =>
      shouldStartConsuming(fromEnd = false, f)
    }

    "start consuming from the end of stream" in { f =>
      shouldStartConsuming(fromEnd = true, f)
    }

    "close producer, kill actor and log error when in trouble" in { f =>
      // given
      val producerProperties = createProducerProperties(f)
      val subscriberProps = createSubscriberProps(f.kafka, producerProperties)
      val supervisor = system.actorOf(Props(new TestHelperSupervisor(self, subscriberProps)))
      val kafkaSubscriberActor = Await.result(supervisor ? "supervise!", atMost = 1 second).asInstanceOf[ActorRef]
      val kafkaSubscriber = ActorSubscriber[String](kafkaSubscriberActor)
      Source(initialDelay = 100 millis, interval = 1 second, tick = "tick").to(Sink(kafkaSubscriber)).run()

      // when
      EventFilter[ProducerClosedException](message = "producer already closed") intercept {
        // let's close the underlying producer
        kafkaSubscriberActor ! "close_producer"
      }

      // then
      expectMsgClass(classOf[Throwable]).getClass should equal(classOf[ProducerClosedException])
    }

    def shouldStartConsuming(fromEnd: Boolean, f: FixtureParam) = {
      // given
      val inputActor = stringSubscriberActor(f)
      val input = ActorSubscriber[String](inputActor)
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
      val inputConsumerActor = f.kafka.consumerActor(props)
      val inputConsumer = ActorPublisher[StringKafkaMessage](inputConsumerActor)

      for (i <- 1 to 2000)
        input.onNext("two")
      inputConsumer.subscribe(output)

      // then
      awaitCond {
        val collectedStrings = Await.result(subscriberActor ? "get elements", atMost = 1 second)
          .asInstanceOf[Seq[StringKafkaMessage]]
        collectedStrings.nonEmpty && collectedStrings.map(_.message()).contains("one") == !fromEnd
      }
      input.onComplete()
      completeProducer(inputActor)
      cancelConsumer(inputConsumerActor)
    }
  }

  def waitUntilFirstElementInQueue(): Unit = {
    // Can't think of any sensible solution at the moment
    Thread.sleep(5000)
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