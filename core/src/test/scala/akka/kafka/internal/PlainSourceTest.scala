/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.lang.Long
import java.util
import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * @author carsten
 */
//noinspection TypeAnnotation
class PlainSourceTest(_system: ActorSystem)
  extends TestKit(_system)
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  def this() = this(ActorSystem())

  implicit val mat = ActorMaterializer()

  private val TOPIC = "topic1"
  private val KEY1 = "key1"

  private def msg(i: Int) = s"message $i"

  it should "stream all available messages" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = PlainSource(consumer)

    for (i <- 1 to 10000) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }

    val results = Await.result(source.take(10000).runWith(Sink.seq), 10.seconds)
    results should have size (10000)
  }

  it should "handle shutdown" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST) {
      var commits = 0
      var wasClosed = false
      override def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]) = {
        super.commitSync(offsets)
        commits += 1
      }
      override def close() = {
        wasClosed = true
      }
    }
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = PlainSource(consumer)

    for (i <- 1 to 9) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }

    val (control, probe) = source.toMat(TestSink.probe[ConsumerRecord[String, String]])(Keep.both).run()
    probe.request(2)
    probe.expectNextN(2)
    val done = control.shutdown()
    probe.expectComplete()
    Await.result(done, 1.second)
    consumer.wasClosed should be (true)
  }

  it should "poll continuously" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = PlainSource(consumer)

    val probe = source.runWith(TestSink.probe[ConsumerRecord[String, String]])
    probe.request(10)
    probe.expectNoMessage(2.seconds)
    for (i <- 1 to 100) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }
    val r1 = probe.expectNextN(10)
    probe.request(100)
    val r2 = probe.expectNextN(90)
    for (i <- 101 to 200) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }
    val r3 = probe.expectNextN(10)
    probe.request(100)
    probe.expectNextN(90)
    probe.cancel()
    Thread.sleep(500)
    consumer.closed() should be (true)
  }

  it should "handle exceptions" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = PlainSource(consumer)
    for (i <- 1 to 100) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }
    consumer.setException(new KafkaException)
    intercept[KafkaException] { Await.result(source.take(10000).runWith(Sink.seq), 10.seconds) }
    consumer.closed() should be (true)
  }
}
