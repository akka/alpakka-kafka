package com.softwaremill.react.kafka

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.ask
import akka.stream.actor._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessages._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, fixture}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec extends TestKit(ActorSystem("ReactiveKafkaIntegrationSpec"))
    with ImplicitSender with fixture.WordSpecLike with Matchers
    with ReactiveKafkaIntegrationTestSupport with MockitoSugar {

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
      shouldCommitOffsets()
    }

    "correctly commit when new flush comes with a single new commit" in { implicit f =>
      // given
      givenQueueWithElements(Seq("1", "2", "3"))

      // then
      val consumerProps = consumerProperties(f)
        .commitInterval(100 millis)
        .noAutoCommit()

      val consumerWithSink = f.kafka.consumeWithOffsetSink(consumerProps)

      // start reading from the queue
      Source(consumerWithSink.publisher)
        .to(consumerWithSink.offsetCommitSink).run()
      Thread.sleep(3000) // wait for flush

      // add one more msg
      val kafkaSubscriberActor = stringSubscriberActor(f)
      Source(List("4"))
        .map(str => ProducerMessage(str, str))
        .to(Sink(ActorSubscriber[StringProducerMessage](kafkaSubscriberActor))).run()
      Thread.sleep(3000) // wait for flush
      completeProducer(kafkaSubscriberActor)

      // then
      var lastReadMsg: Option[String] = None
      val consumerActor = f.kafka.consumerActor(consumerProps)
      Source(ActorPublisher[StringConsumerRecord](consumerActor))
        .map(_.value())
        .runWith(Sink.foreach{
          m => lastReadMsg = Some(m)
        })
      Thread.sleep(3000) // wait for consumption of eventual additional element

      completeProducer(kafkaSubscriberActor)
      consumerWithSink.cancel()
      Thread.sleep(3000) // wait for complete

      // kill the consumer
      cancelConsumer(consumerActor)
      lastReadMsg shouldBe None
    }

    "start consuming from the beginning of stream" in { implicit f =>
      shouldStartConsuming(fromEnd = false)
    }

    "start consuming from the end of stream" in { implicit f =>
      shouldStartConsuming(fromEnd = true)
    }

    "Log error and supervise when in trouble" in { f =>
      // given
      val producerProperties = createProducerProperties(f)
      val subscriberProps = createSubscriberProps(f.kafka, producerProperties)
      val supervisor = system.actorOf(Props(new TestHelperSupervisor(self, subscriberProps)))
      val kafkaSubscriberActor = Await.result(supervisor ? "supervise!", atMost = 1 second).asInstanceOf[ActorRef]
      watch(kafkaSubscriberActor)
      val nonStringMsg = Array.fill(5)('0'.toByte)
      val kafkaSubscriber = ActorSubscriber[ProducerMessage[String, Array[Byte]]](kafkaSubscriberActor)
      Source(initialDelay = 100 millis, interval = 1000 millis, tick = nonStringMsg)
        .map(bytes => ProducerMessage("key", bytes))
        .to(Sink(kafkaSubscriber)).run()

      // then
      expectMsgClass(classOf[Throwable]).getClass should equal(classOf[SerializationException])
      expectTerminated(kafkaSubscriberActor)
    }

    "allow Source supervision" in { implicit f =>
      // given
      givenQueueWithElements(Seq("test", "a", "b"))
      var msgs = List.empty[StringConsumerRecord]

      val publisher = f.kafka.consume(consumerProperties(f))
      Source(publisher)
        .map({ msg =>
          msgs = msgs :+ msg
          msg.value()
        })
        .runWith(TestSink.probe[String])
        .request(1)
        .expectNext("test")

      val kafkaPublisherActor = {
        val consumer = mock[KafkaConsumer[String, String]]

        Mockito.when(consumer.poll(1000))
          .thenReturn(consumerRecords(new ConsumerRecord("topic", 1, 1L, "key", "value")))
          .thenReturn(consumerRecords(new ConsumerRecord("topic", 1, 2L, "key", "value2")))
          .thenReturn(consumerRecords(new ConsumerRecord("topic", 1, 3L, "key", "value3")))
          .thenThrow(new KafkaException("Kafka stub disconnected"))

        val akkaConsumerProps = Props(new KafkaActorPublisher(consumer))
        system.actorOf(akkaConsumerProps.withDispatcher(ReactiveKafka.ConsumerDefaultDispatcher))
      }
      val kafkaPublisher = ActorPublisher[StringConsumerRecord](kafkaPublisherActor)
      watch(kafkaPublisherActor)

      // when
      Source(kafkaPublisher).to(Sink.ignore).run()

      // then
      expectTerminated(kafkaPublisherActor)
    }

    def consumerRecords(record: StringConsumerRecord) = {
      new ConsumerRecords(Map[TopicPartition, java.util.List[StringConsumerRecord]](
        new TopicPartition("topic", 1) -> List(record).asJava
      ).asJava)
    }

    def shouldStartConsuming(fromEnd: Boolean)(implicit f: FixtureParam) = {
      val inputActor = stringSubscriberActor(f)
      val input = ActorSubscriber[StringProducerMessage](inputActor)
      input.onNext(ProducerMessage("one"))
      input.onNext(ProducerMessage("two"))
      waitUntilElementsInQueue()
      verifyQueueHas(Seq("one", "two"))

      val buffer: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()

      // start consumer stream
      val properties = if (fromEnd)
        consumerProperties(f).readFromEndOfStream()
      else
        consumerProperties(f)

      val consumer = f.kafka.consume(properties)(system)
      Source(consumer)
        .map({
          m =>
            buffer.add(m.value())
            m
        })
        .runWith(Sink.ignore)
      Thread.sleep(10000) // wait till it starts sucking
      input.onNext(ProducerMessage("three"))

      if (fromEnd) {
        awaitCond(buffer.iterator().asScala.toList == List("three"))
        verifyNever(buffer.contains("one"))
      }
      else {
        awaitCond(buffer.iterator().asScala.toList == List("one", "two", "three"))
      }

    }
  }

  def waitUntilElementsInQueue(): Unit = {
    // Can't think of any sensible solution at the moment
    Thread.sleep(5000)
  }

}

class ReactiveTestSubscriber extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  var elements: Vector[StringConsumerRecord] = Vector.empty

  def receive = {

    case ActorSubscriberMessage.OnNext(element) =>
      elements = elements :+ element.asInstanceOf[StringConsumerRecord]
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