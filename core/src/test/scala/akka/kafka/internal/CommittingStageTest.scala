/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.lang.Long
import java.util

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * @author carsten
 */
//noinspection TypeAnnotation
class CommittingStageTest(_system: ActorSystem)
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

  it should "stream all available events" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = CommittingStage(consumer, Flow[ConsumerRecord[String, String]].map(identity), 1)

    for (i <- 1 to 10000) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }

    val probe = source.runWith(TestSink.probe[ConsumerRecord[String, String]])
    probe.request(10000)
    val results = probe.expectNextN(10000)
    results should have size (10000)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (10000)
    probe.cancel()
    Thread.sleep(500)
    consumer.closed() should be (true)
  }
  it should "commit only in batches" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST) {
      var commits = 0
      override def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]) = {
        super.commitSync(offsets)
        commits += 1
      }
    }
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = CommittingStage(consumer, Flow[ConsumerRecord[String, String]].map(identity), 2)

    for (i <- 1 to 10) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }

    val probe = source.runWith(TestSink.probe[ConsumerRecord[String, String]])
    probe.request(10)
    val results = probe.expectNextN(10)
    results should have size (10)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (10)
    probe.cancel()
    Thread.sleep(500)
    consumer.closed() should be (true)
    consumer.commits should be (5)
  }
  it should "commit after close" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST) {
      var commits = 0
      override def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]) = {
        super.commitSync(offsets)
        commits += 1
      }
      override def close() = {}
    }
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = CommittingStage(consumer, Flow[ConsumerRecord[String, String]].map(identity), 2)

    for (i <- 1 to 9) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }

    val probe = source.runWith(TestSink.probe[ConsumerRecord[String, String]])
    probe.request(10)
    val results = probe.expectNextN(9)
    probe.cancel()
    Thread.sleep(500)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (9)
    consumer.commits should be (5)
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
    val source = CommittingStage(consumer, Flow[ConsumerRecord[String, String]].map(identity), 2)

    for (i <- 1 to 9) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }

    val (control, probe) = source.toMat(TestSink.probe[ConsumerRecord[String, String]])(Keep.both).run()
    probe.request(2)
    probe.expectNextN(2)
    val done = control.shutdown()
    probe.expectComplete()
    Await.result(done, 1.second)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (2)
    consumer.wasClosed should be (true)
  }
  it should "commit continuously" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST) {
      var commits = 0
      override def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]) = {
        super.commitSync(offsets)
        commits += 1
      }
      override def close() = {}
    }
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = CommittingStage(consumer, Flow[ConsumerRecord[String, String]].map(identity), 2)

    Thread.sleep(2000)

    consumer.commits should be (0)

    consumer.addRecord(new ConsumerRecord(TOPIC, 0, 0, KEY1, msg(0)))

    val probe = source.runWith(TestSink.probe[ConsumerRecord[String, String]])
    probe.request(10)
    val results = probe.expectNextN(1)
    Thread.sleep(2000)
    consumer.commits should be (1)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (1)
    probe.cancel()
  }
  it should "poll continuously" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = CommittingStage(consumer, Flow[ConsumerRecord[String, String]].map(identity), 1)

    val probe = source.runWith(TestSink.probe[ConsumerRecord[String, String]])
    probe.request(10)
    probe.expectNoMessage(2.seconds)
    for (i <- 1 to 100) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }
    probe.expectNextN(10)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (10)
    probe.request(100)
    probe.expectNextN(90)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (100)
    for (i <- 101 to 200) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(1)))
    }
    probe.expectNextN(10)
    probe.request(100)
    probe.expectNextN(90)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (200)
    probe.cancel()
    Thread.sleep(500)
    consumer.closed() should be (true)
  }
  it should "handle exceptions" in {
    val consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST) {
      override def close() = {}
    }
    consumer.assign(util.Arrays.asList(new TopicPartition(TOPIC, 0)))
    consumer.updateBeginningOffsets(Map(new TopicPartition(TOPIC, 0) -> new Long(0)).asJava)
    val source = CommittingStage(consumer, Flow[ConsumerRecord[String, String]].map(identity), 1)

    for (i <- 1 to 100) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }

    val probe = source.runWith(TestSink.probe[ConsumerRecord[String, String]])
    probe.request(50)
    probe.expectNextN(50)

    for (i <- 101 to 200) {
      consumer.addRecord(new ConsumerRecord(TOPIC, 0, i - 1, KEY1, msg(i)))
    }
    consumer.setException(new KafkaException)
    probe.request(100)
    probe.expectNextN(50)
    probe.expectError().isInstanceOf[KafkaException] should be (true)
    consumer.committed(new TopicPartition(TOPIC, 0)).offset() should equal (100)
    consumer.close()
  }
}
