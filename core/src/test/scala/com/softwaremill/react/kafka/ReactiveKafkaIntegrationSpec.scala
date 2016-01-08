package com.softwaremill.react.kafka

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import akka.actor._
import akka.stream.actor._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessages._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, fixture}

import scala.collection.JavaConverters._
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
      givenInitializedTopic()
      val msgs = Seq("a", "b", "c")
      val sink: Sink[ProducerMessage[Array[Byte], String], Unit] = Sink.fromGraph(stringGraphSink(f))
      // when
      val (probe, _) = TestSource.probe[String]
        .map(ProducerMessage(_))
        .toMat(sink)(Keep.both)
        .run()
      msgs.foreach(probe.sendNext)
      probe.sendComplete()

      // then
      val sourceStage = createSourceStage(f, consumerProperties(f))
      Source.fromGraph(sourceStage)
        .map(_.value())
        .runWith(TestSink.probe[String])
        .request(msgs.length.toLong + 1)
        .expectNext(InitialMsg, msgs.head, msgs.tail: _*)
        .cancel()
    }

    "start consuming from the beginning of stream" in { implicit f =>
      shouldStartConsuming(fromEnd = false)
    }

    "start consuming from the end of stream" in { implicit f =>
      shouldStartConsuming(fromEnd = true)
    }

    def shouldStartConsuming(fromEnd: Boolean)(implicit f: FixtureParam) = {
      val producerProps = createProducerProperties(f)
      val producer = new KafkaProducer(producerProps.rawProperties, producerProps.keySerializer, producerProps.valueSerializer)
      producer.send(new ProducerRecord(producerProps.topic, "one"))
      producer.send(new ProducerRecord(producerProps.topic, "two"))

      val buffer: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()

      // start consumer stream
      val properties = if (fromEnd)
        consumerProperties(f).readFromEndOfStream()
      else
        consumerProperties(f)

      val sourceGraph = createSourceStage(f, properties)
      Source.fromGraph(sourceGraph)
        .map({
          m =>
            buffer.add(m.value())
            m
        })
        .runWith(Sink.ignore)
      Thread.sleep(10000) // wait till it starts sucking
      producer.send(new ProducerRecord(producerProps.topic, "three"))
      producer.close(2000, TimeUnit.SECONDS)

      if (fromEnd) {
        awaitCond(buffer.iterator().asScala.toList == List("three"))
        verifyNever(buffer.contains("one"))
      }
      else {
        awaitCond(buffer.iterator().asScala.toList == List("one", "two", "three"))
      }
    }
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