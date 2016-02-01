package com.softwaremill.react.kafka

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, fixture}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec
    extends TestKit(ActorSystem(
      "ReactiveKafkaIntegrationSpec",
      ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")
    ))
    with ImplicitSender with fixture.WordSpecLike with Matchers with GraphBasedIntegrationTestSupport with MockitoSugar {

  implicit val timeout = Timeout(1 second)

  def partitionizer(in: String): Option[Array[Byte]] = Some(in.hashCode().toString.getBytes)

  "Reactive kafka streams" must {
    //    "publish and consume" in { implicit f =>
    //      // given
    //      givenInitializedTopic()
    //      val msgs = Seq("a", "b", "c")
    //      val sink = stringGraphSink(f)
    //      // when
    //      val (probe, _) = TestSource.probe[String]
    //        .toMat(sink)(Keep.both)
    //        .run()
    //      msgs.foreach(probe.sendNext)
    //      probe.sendComplete()
    //
    //      // then
    //      val source = createSource(f, consumerProperties(f))
    //      source
    //        .map(_.message())
    //        .runWith(TestSink.probe[String])
    //        .request(msgs.length.toLong + 1)
    //        .expectNext(InitialMsg, msgs.head, msgs.tail: _*)
    //        .cancel()
    //    }
    //
    //    "start consuming from the beginning of stream" in { implicit f =>
    //      shouldStartConsuming(fromEnd = false)
    //    }
    //
    //    "start consuming from the end of stream" in { implicit f =>
    //      shouldStartConsuming(fromEnd = true)
    //    }

    "commit offsets manually" in { implicit f =>
      // given
      givenQueueWithElements(Seq("0", "1", "2", "3", "4", "5"))

      // when
      val consumerProps = consumerProperties(f)
        .kafkaOffsetsStorage()
        .commitInterval(300 millis)

      val consumerWithSink = f.kafka.sourceWithOffsetSink(consumerProps)

      consumerWithSink.source
        .filter(_.message().toInt < 3)
        .to(consumerWithSink.offsetCommitSink)
        .run()

      Thread.sleep(5000) // wait for flush
      consumerWithSink.underlyingConsumer.close()
      Thread.sleep(3000) // wait for consumer to close

      // then
      // this has to be run after ensuring closed committing consumer (we need the same consumer group id)
      verifyQueueHas(Seq("3", "4", "5"))
    }

    def shouldStartConsuming(fromEnd: Boolean)(implicit f: FixtureParam) = {
      val producerProps = createProducerProperties(f)
      val producer = new Producer[Any, Any](producerProps.toProducerConfig)
      producer.send(new KeyedMessage(producerProps.topic, "one".getBytes))
      producer.send(new KeyedMessage(producerProps.topic, "two".getBytes))

      val buffer: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()

      // start consumer stream
      val properties = if (fromEnd)
        consumerProperties(f).readFromEndOfStream()
      else
        consumerProperties(f)

      val source = createSource(f, properties)
      source
        .map({
          m =>
            buffer.add(m.message())
            m
        })
        .runWith(Sink.ignore)
      Thread.sleep(10000) // wait till it starts sucking
      producer.send(new KeyedMessage(producerProps.topic, "three".getBytes))
      producer.close()

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