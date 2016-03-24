/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer.CommittableMessage
import akka.kafka.scaladsl.Consumer.Control
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.BeforeAndAfterAll
import akka.kafka.scaladsl.Consumer.ClientTopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import akka.kafka.ConsumerSettings

object ConsumerTest {
  type K = String
  type V = String
  type Record = ConsumerRecord[K, V]

  def createMessage(seed: Int): Consumer.CommittableMessage[K, V] = createMessage(seed, "topic")

  def createMessage(seed: Int, topic: String, clientId: String = "client1"): Consumer.CommittableMessage[K, V] = {
    val offset = Consumer.PartitionOffset(ClientTopicPartition(clientId, topic, 1), seed.toLong)
    Consumer.CommittableMessage(seed.toString, seed.toString, ConsumerStage.CommittableOffsetImpl(offset)(null))
  }

}

class ConsumerTest(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import ConsumerTest._

  def this() = this(ActorSystem())

  override def afterAll(): Unit = {
    shutdown(system)
  }

  implicit val m = ActorMaterializer(ActorMaterializerSettings(_system).withFuzzing(true))
  implicit val ec = _system.dispatcher

  val settings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer, Set("topic"))

  def testSource(mock: ConsumerMock[K, V], clientId: String = "client1"): Source[CommittableMessage[K, V], Control] = {
    Source.fromGraph(new CommittableConsumerStage[K, V](settings.withClientId(clientId), () => mock.mock))
  }

  it should "complete stage when stream control.stop called" in {
    val mock = new ConsumerMock[K, V]()
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    probe.request(100)

    Await.result(control.stop(), remainingOrDefault)
    probe.expectComplete()
    mock.verifyClosed()
  }

  it should "complete stage when processing flow canceled" in {
    val mock = new ConsumerMock[K, V]()
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    probe.request(100)
    mock.verifyClosed(never())
    probe.cancel()
    Await.result(control.stopped, remainingOrDefault)
    mock.verifyClosed()
  }

  def toRecord(msg: Consumer.CommittableMessage[K, V]): ConsumerRecord[K, V] = {
    val offset = msg.committableOffset.partitionOffset
    new ConsumerRecord(offset.key.topic, offset.key.partition, offset.offset, msg.key, msg.value)
  }

  def checkMessagesReceiving(msgss: Seq[Seq[Consumer.CommittableMessage[K, V]]]): Unit = {
    val mock = new ConsumerMock[K, V]()
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    probe.request(msgss.map(_.size).sum.toLong)
    msgss.foreach(chunk => mock.enqueue(chunk.map(toRecord)))
    probe.expectNextN(msgss.flatten)

    Await.result(control.stop(), remainingOrDefault)
  }

  val messages = (1 to 10000).map(createMessage)
  it should "emit messages received as one big chunk" in {
    checkMessagesReceiving(Seq(messages))
  }

  it should "emit messages received as medium chunks" in {
    checkMessagesReceiving(messages.grouped(97).to[Seq])
  }

  it should "emit messages received as one message per chunk" in {
    checkMessagesReceiving(messages.grouped(1).to[Seq])
  }

  it should "emit messages received with empty some messages" in {
    checkMessagesReceiving(
      messages
      .grouped(97)
      .map(x => Seq(Seq.empty, x))
      .flatten
      .to[Seq]
    )
  }

  it should "call commitAsync for commit message and then complete future" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    probe.request(100)
    val done = probe.expectNext().committableOffset.commit()

    awaitAssert {
      commitLog.calls should have size (1)
    }
    val (topicPartition, offsetMeta) = commitLog.calls.head._1.head
    topicPartition.topic should ===(msg.partitionOffset.key.topic)
    topicPartition.partition should ===(msg.partitionOffset.key.partition)
    // committed offset should be the next message the application will consume, i.e. +1
    offsetMeta.offset should ===(msg.partitionOffset.offset + 1)

    //emulate commit
    commitLog.calls.head match {
      case (offsets, callback) => callback.onComplete(offsets.asJava, null)
    }

    Await.result(done, remainingOrDefault)
    Await.result(control.stop(), remainingOrDefault)
  }

  it should "fail future in case of commit fail" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    probe.request(100)
    val done = probe.expectNext().committableOffset.commit()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    //emulate commit failure
    val failure = new Exception()
    commitLog.calls.head match {
      case (offsets, callback) => callback.onComplete(null, failure)
    }

    intercept[Exception] {
      Await.result(done, remainingOrDefault)
    } should be(failure)
    Await.result(control.stop(), remainingOrDefault)
  }

  it should "call commitAsync for every commit message (no commit batching)" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msgs = (1 to 100).map(createMessage)
    mock.enqueue(msgs.map(toRecord))

    probe.request(100)
    val done = Future.sequence(probe.expectNextN(100).map(_.committableOffset.commit()))

    awaitAssert {
      commitLog.calls should have size (100)
    }

    //emulate commit
    commitLog.calls.map {
      case (offsets, callback) => callback.onComplete(offsets.asJava, null)
    }

    Await.result(done, remainingOrDefault)
    Await.result(control.stop(), remainingOrDefault)
  }

  it should "support commit batching" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msgsTopic1 = (1 to 3).map(createMessage(_, "topic1"))
    val msgsTopic2 = (11 to 13).map(createMessage(_, "topic2"))
    mock.enqueue(msgsTopic1.map(toRecord))
    mock.enqueue(msgsTopic2.map(toRecord))

    probe.request(100)
    val batch = probe.expectNextN(6).map(_.committableOffset)
      .foldLeft(CommittableOffsetBatch.empty) { (b, c) => b.updated(c) }

    val done = batch.commit()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val commitMap = commitLog.calls.head._1
    commitMap(new TopicPartition("topic1", 1)).offset should ===(msgsTopic1.last.partitionOffset.offset + 1)
    commitMap(new TopicPartition("topic2", 1)).offset should ===(msgsTopic2.last.partitionOffset.offset + 1)

    //emulate commit
    commitLog.calls.map {
      case (offsets, callback) => callback.onComplete(offsets.asJava, null)
    }

    Await.result(done, remainingOrDefault)
    Await.result(control.stop(), remainingOrDefault)
  }

  it should "support commit batching from more than one stage" in {
    val commitLog1 = new ConsumerMock.LogHandler()
    val commitLog2 = new ConsumerMock.LogHandler()
    val mock1 = new ConsumerMock[K, V](commitLog1)
    val mock2 = new ConsumerMock[K, V](commitLog2)
    val (control1, probe1) = testSource(mock1, "client1")
      .toMat(TestSink.probe)(Keep.both)
      .run()
    val (control2, probe2) = testSource(mock2, "client2")
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msgs1a = (1 to 3).map(createMessage(_, "topic1", "client1"))
    val msgs1b = (11 to 13).map(createMessage(_, "topic2", "client1"))
    mock1.enqueue(msgs1a.map(toRecord))
    mock1.enqueue(msgs1b.map(toRecord))

    val msgs2a = (1 to 3).map(createMessage(_, "topic1", "client2"))
    val msgs2b = (11 to 13).map(createMessage(_, "topic3", "client2"))
    mock2.enqueue(msgs2a.map(toRecord))
    mock2.enqueue(msgs2b.map(toRecord))

    probe1.request(100)
    probe2.request(100)

    val batch1 = probe1.expectNextN(6).map(_.committableOffset)
      .foldLeft(CommittableOffsetBatch.empty) { (b, c) => b.updated(c) }

    val batch2 = probe2.expectNextN(6).map(_.committableOffset)
      .foldLeft(batch1) { (b, c) => b.updated(c) }

    val done2 = batch2.commit()

    awaitAssert {
      commitLog1.calls should have size (1)
      commitLog2.calls should have size (1)
    }

    val commitMap1 = commitLog1.calls.head._1
    commitMap1(new TopicPartition("topic1", 1)).offset should ===(msgs1a.last.partitionOffset.offset + 1)
    commitMap1(new TopicPartition("topic2", 1)).offset should ===(msgs1b.last.partitionOffset.offset + 1)

    val commitMap2 = commitLog2.calls.head._1
    commitMap2(new TopicPartition("topic1", 1)).offset should ===(msgs2a.last.partitionOffset.offset + 1)
    commitMap2(new TopicPartition("topic3", 1)).offset should ===(msgs2b.last.partitionOffset.offset + 1)

    //emulate commit
    commitLog1.calls.map {
      case (offsets, callback) => callback.onComplete(offsets.asJava, null)
    }
    commitLog2.calls.map {
      case (offsets, callback) => callback.onComplete(offsets.asJava, null)
    }

    Await.result(done2, remainingOrDefault)
    Await.result(control1.stop(), remainingOrDefault)
    Await.result(control2.stop(), remainingOrDefault)
  }

  it should "keep stage running until all futures completed" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msgs = (1 to 10).map(createMessage)
    mock.enqueue(msgs.map(toRecord))

    probe.request(100)
    val done = probe.expectNext().committableOffset.commit()
    val rest = probe.expectNextN(9)

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val stopped = control.stop()
    probe.expectComplete()
    stopped.isCompleted should ===(false)

    //emulate commit
    commitLog.calls.map {
      case (offsets, callback) => callback.onComplete(offsets.asJava, null)
    }

    Await.result(done, remainingOrDefault)
    Await.result(stopped, remainingOrDefault)
    mock.verifyClosed()
  }

  it should "complete futures with failure when commit after stop" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    probe.request(100)
    val first = probe.expectNext()

    val stopped = control.stop()
    probe.expectComplete()
    Await.result(stopped, remainingOrDefault)

    val done = first.committableOffset.commit()
    intercept[IllegalStateException] {
      Await.result(done, remainingOrDefault)
    }
  }

  it should "keep stage running after cancellation until all futures completed" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val msgs = (1 to 10).map(createMessage)
    mock.enqueue(msgs.map(toRecord))

    probe.request(5)
    val done = probe.expectNext().committableOffset.commit()
    val more = probe.expectNextN(4)

    awaitAssert {
      commitLog.calls should have size (1)
    }

    probe.cancel()
    probe.expectNoMsg(200.millis)
    control.stopped.isCompleted should ===(false)

    //emulate commit
    commitLog.calls.map {
      case (offsets, callback) => callback.onComplete(offsets.asJava, null)
    }

    Await.result(done, remainingOrDefault)
    Await.result(control.stopped, remainingOrDefault)
    mock.verifyClosed()
  }
}

object ConsumerMock {
  type CommitHandler = (Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback) => Unit

  def notImplementedHandler: CommitHandler = (_, _) => ???

  class LogHandler extends CommitHandler {
    var calls: Seq[(Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback)] = Seq.empty
    def apply(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback) = {
      calls :+= ((offsets, callback))
    }
  }
}

class ConsumerMock[K, V](handler: ConsumerMock.CommitHandler = ConsumerMock.notImplementedHandler) {
  private var actions = collection.immutable.Queue.empty[Seq[ConsumerRecord[K, V]]]

  val mock = {
    val result = Mockito.mock(classOf[KafkaConsumer[K, V]])
    Mockito.when(result.poll(mockito.Matchers.any[Long])).thenAnswer(new Answer[ConsumerRecords[K, V]] {
      override def answer(invocation: InvocationOnMock) = ConsumerMock.this.synchronized {
        val records = actions.dequeueOption.map {
          case (element, remains) =>
            actions = remains
            element
              .groupBy(x => new TopicPartition(x.topic(), x.partition()))
              .map {
                case (topicPart, messages) => (topicPart, messages.asJava)
              }
        }.getOrElse(Map.empty)
        new ConsumerRecords[K, V](records.asJava)
      }
    })
    Mockito.when(result.commitAsync(mockito.Matchers.any[JMap[TopicPartition, OffsetAndMetadata]], mockito.Matchers.any[OffsetCommitCallback])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        import scala.collection.JavaConversions._
        val offsets = invocation.getArgumentAt(0, classOf[JMap[TopicPartition, OffsetAndMetadata]])
        val callback = invocation.getArgumentAt(1, classOf[OffsetCommitCallback])
        handler(mapAsScalaMap(offsets).toMap, callback)
        ()
      }
    })
    result
  }

  def enqueue(records: Seq[ConsumerRecord[K, V]]) = {
    synchronized {
      actions :+= records
    }
  }

  def verifyClosed(mode: VerificationMode = Mockito.times(1)) = {
    verify(mock, mode).close()
  }

  def verifyPoll(mode: VerificationMode = Mockito.atLeastOnce()) = {
    verify(mock, mode).poll(mockito.Matchers.any[Long])
  }
}
