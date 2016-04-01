package com.softwaremill.react.kafka

import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import akka.actor._
import akka.stream.actor._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.commit.CommitSink
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.PatienceConfiguration.Interval
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, fixture}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

class ReactiveKafkaIntegrationSpec extends TestKit(ActorSystem("ReactiveKafkaIntegrationSpec"))
  with ImplicitSender with fixture.WordSpecLike with Matchers
  with ReactiveKafkaIntegrationTestSupport with MockitoSugar {

  implicit val timeout = Timeout(1 second)

  def partitionizer(in: String): Option[Array[Byte]] = Some(in.hashCode().toString.getBytes)

  "Reactive kafka streams" must {
    "publish and consume" in { implicit f =>
      // given
      givenInitializedTopic()
      val msgs = Seq("a", "b", "c")
      val sink = stringGraphSink(f)
      // when
      val (probe, _) = TestSource.probe[String]
        .map(ProducerMessage(_))
        .toMat(sink)(Keep.both)
        .run()
      msgs.foreach(probe.sendNext)
      probe.sendComplete()

      // then
      val source = createSource(f, consumerProperties(f))
      source
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

    "commit offsets manually" in { implicit f =>
      // given
      givenQueueWithElements(Seq("0", "1", "2", "3", "4", "5"))

      // when
      val consumerProps = consumerProperties(f)
        .commitInterval(1000 millis)

      val consumerWithSink = f.kafka.sourceWithOffsetSink(consumerProps)

      consumerWithSink.source
        .filter(_.value().toInt < 3)
        .to(consumerWithSink.offsetCommitSink)
        .run()

      Thread.sleep(5000) // wait for flush
      consumerWithSink.underlyingConsumer.close()
      Thread.sleep(3000) // wait for consumer to close

      // then
      // this has to be run after ensuring closed committing consumer (we need the same consumer group id)
      verifyQueueHas(Seq("3", "4", "5"))
    }

    "not commit the same offsets twice" in { implicit f =>
      // given
      val consumerProps = consumerProperties(f)
        .commitInterval(700 millis)
        .noAutoCommit()
      val mockConsumer = new CommitCountingMockConsumer[String, String]
      val topicPartition1 = new TopicPartition(f.topic, 0)
      val topicPartition2 = new TopicPartition(f.topic, 1)
      mockConsumer.updateBeginningOffsets(Map(topicPartition1 -> java.lang.Long.valueOf(0L), topicPartition2 -> java.lang.Long.valueOf(0L)).asJava)
      val mockProvider = (_: ConsumerProperties[String, String]) => mockConsumer
      val reactiveConsumer: ReactiveKafkaConsumer[String, String] = new ReactiveKafkaConsumer(consumerProps, Set(topicPartition1, topicPartition2), Map(), mockProvider)
      val propsWithConsumer = ConsumerWithActorProps(reactiveConsumer, Props(new KafkaActorPublisher(reactiveConsumer)))
      val actor = system.actorOf(propsWithConsumer.actorProps)
      val publisherWithSink = PublisherWithCommitSink[String, String](
        ActorPublisher[ConsumerRecord[String, String]](actor), actor, CommitSink.create(actor, consumerProps)
      )
      val source = Source.fromPublisher(publisherWithSink.publisher)
      for (i <- 0 to 9) {
        mockConsumer.addRecord(new ConsumerRecord[String, String](f.topic, 0, i.toLong, s"k$i", i.toString))
      }

      // when
      source
        .filter(_.value().toInt < 3)
        .to(publisherWithSink.offsetCommitSink)
        .run()

      // then
      eventually(PatienceConfiguration.Timeout(Span(5, Seconds)), Interval(Span(150, Millis))) {
        mockConsumer.committed(topicPartition1) should be(new OffsetAndMetadata(3))
      }

      // trigger one more commit by adding a new record to the second partition
      // this should result in committing only the new metadata
      mockConsumer.addRecord(new ConsumerRecord[String, String](f.topic, 1, 10, s"k10", "1"))

      verifyNever {
        val counter = mockConsumer.counter.getOrElse(topicPartition1 -> new OffsetAndMetadata(3), 0)
        counter != 1
      }
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

      val source = createSource(f, properties)
      source
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

class CommitCountingMockConsumer[K, V] extends MockConsumer[K, V](OffsetResetStrategy.EARLIEST) {
  val counter = mutable.Map[(TopicPartition, OffsetAndMetadata), Int]()

  override def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    for (o <- offsets.asScala) {
      counter += o -> (counter.getOrElse(o, 0) + 1)
    }
    super.commitSync(offsets)
  }

  override def poll(timeout: Long): ConsumerRecords[K, V] = {
    synchronized {
      super.poll(timeout)
    }
  }

  override def addRecord(record: ConsumerRecord[K, V]): Unit = {
    synchronized {
      super.addRecord(record)
    }
  }
}