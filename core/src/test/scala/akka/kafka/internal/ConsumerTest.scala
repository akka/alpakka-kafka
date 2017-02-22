/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.{List => JList, Map => JMap, Set => JSet}

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage._
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions.TopicSubscription
import akka.kafka.scaladsl.Consumer, Consumer.Control
import akka.pattern.AskTimeoutException
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.stream.contrib.TestKit.assertAllStagesStopped

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import org.mockito, mockito.Mockito, Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode

import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object ConsumerTest {
  type K = String
  type V = String
  type Record = ConsumerRecord[K, V]

  def createMessage(seed: Int): CommittableMessage[K, V] = createMessage(seed, "topic")

  def createMessage(seed: Int, topic: String, groupId: String = "group1"): CommittableMessage[K, V] = {
    val offset = PartitionOffset(GroupTopicPartition(groupId, topic, 1), seed.toLong)
    val record = new ConsumerRecord(offset.key.topic, offset.key.partition, offset.offset, seed.toString, seed.toString)
    CommittableMessage(record, ConsumerStage.CommittableOffsetImpl(offset)(null))
  }

  def toRecord(msg: CommittableMessage[K, V]): ConsumerRecord[K, V] = msg.record
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
  val messages = (1 to 10000).map(createMessage)

  def checkMessagesReceiving(msgss: Seq[Seq[CommittableMessage[K, V]]]): Unit = {
    val mock = new ConsumerMock[K, V]()
    val (control, probe) = testSource(mock)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    probe.request(msgss.map(_.size).sum.toLong)
    msgss.foreach(chunk => mock.enqueue(chunk.map(toRecord)))
    probe.expectNextN(msgss.flatten)

    Await.result(control.shutdown(), remainingOrDefault)
  }

  def testSource(mock: ConsumerMock[K, V], groupId: String = "group1", topics: Set[String] = Set("topic")): Source[CommittableMessage[K, V], Control] = {
    val settings = new ConsumerSettings(Map(ConsumerConfig.GROUP_ID_CONFIG -> groupId), Some(new StringDeserializer), Some(new StringDeserializer),
      1.milli, 1.milli, 1.second, 1.second, 1.second, 5.seconds, 3, "akka.kafka.default-dispatcher") {
      override def createKafkaConsumer(): KafkaConsumer[K, V] = {
        mock.mock
      }
    }
    Consumer.committableSource(settings, TopicSubscription(topics))
  }

  it should "fail stream when poll() fails with unhandled exception" in {
    assertAllStagesStopped {
      val mock = new FailingConsumerMock[K, V](new Exception("Fatal Kafka error"), failOnCallNumber = 1)

      val probe = testSource(mock)
        .toMat(TestSink.probe)(Keep.right)
        .run()

      probe
        .request(1)
        .expectError()
    }
  }

  it should "not fail stream when poll() fails twice with WakeupException" in {
    assertAllStagesStopped {
      val mock = new FailingConsumerMock[K, V](new WakeupException(), failOnCallNumber = 1, 2)

      val probe = testSource(mock)
        .toMat(TestSink.probe)(Keep.right)
        .run()

      probe
        .request(1)
        .expectNoMsg()
        .cancel()
    }
  }

  it should "not fail stream when poll() fails twice, then succeeds, then fails twice with WakeupException" in assertAllStagesStopped {
    val mock = new FailingConsumerMock[K, V](new WakeupException(), failOnCallNumber = 1, 2, 4, 5)

    val probe = testSource(mock)
      .toMat(TestSink.probe)(Keep.right)
      .run()

    probe
      .request(1)
      .expectNoMsg()
      .cancel()
  }

  it should "not fail stream when poll() fail limit exceeded" in {
    assertAllStagesStopped {
      val mock = new FailingConsumerMock[K, V](new WakeupException(), failOnCallNumber = 1, 2, 3)

      val probe = testSource(mock)
        .toMat(TestSink.probe)(Keep.right)
        .run()

      probe
        .request(1)
        .expectError()
    }
  }

  it should "complete stage when stream control.stop called" in {
    assertAllStagesStopped {
      val mock = new ConsumerMock[K, V]()
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe.request(100)

      Await.result(control.shutdown(), remainingOrDefault)
      probe.expectComplete()
      mock.verifyClosed()
    }
  }

  it should "complete stage when processing flow canceled" in {
    assertAllStagesStopped {
      val mock = new ConsumerMock[K, V]()
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe.request(100)
      mock.verifyClosed(never())
      probe.cancel()
      Await.result(control.isShutdown, remainingOrDefault)
      mock.verifyClosed()
    }
  }

  it should "emit messages received as one big chunk" in {
    assertAllStagesStopped {
      checkMessagesReceiving(Seq(messages))
    }
  }

  it should "emit messages received as medium chunks" in {
    assertAllStagesStopped {
      checkMessagesReceiving(messages.grouped(97).to[Seq])
    }
  }

  it should "emit messages received as one message per chunk" in {
    assertAllStagesStopped {
      checkMessagesReceiving(messages.grouped(1).to[Seq])
    }
  }

  it should "emit messages received with empty some messages" in {
    assertAllStagesStopped {
      checkMessagesReceiving(
        messages
        .grouped(97)
        .map(x => Seq(Seq.empty, x))
        .flatten
        .to[Seq]
      )
    }
  }

  it should "call commitAsync for commit message and then complete future" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msg = createMessage(1)
      mock.enqueue(List(toRecord(msg)))

      probe.request(100)
      val done = probe.expectNext().committableOffset.commitScaladsl()

      awaitAssert {
        commitLog.calls should have size (1)
      }

      val (topicPartition, offsetMeta) = commitLog.calls.head._1.head
      topicPartition.topic should ===(msg.record.topic())
      topicPartition.partition should ===(msg.record.partition())
      // committed offset should be the next message the application will consume, i.e. +1
      offsetMeta.offset should ===(msg.record.offset() + 1)

      //emulate commit
      commitLog.calls.head match {
        case (offsets, callback) => callback.onComplete(offsets.asJava, null)
      }

      Await.result(done, remainingOrDefault)
      Await.result(control.shutdown(), remainingOrDefault)
    }
  }

  it should "fail future in case of commit fail" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msg = createMessage(1)
      mock.enqueue(List(toRecord(msg)))

      probe.request(100)
      val done = probe.expectNext().committableOffset.commitScaladsl()

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
      Await.result(control.shutdown(), remainingOrDefault)
    }
  }

  it should "call commitAsync for every commit message (no commit batching)" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msgs = (1 to 100).map(createMessage)
      mock.enqueue(msgs.map(toRecord))

      probe.request(100)
      val done = Future.sequence(probe.expectNextN(100).map(_.committableOffset.commitScaladsl()))

      awaitAssert {
        commitLog.calls should have size (100)
      }

      //emulate commit
      commitLog.calls.map {
        case (offsets, callback) => callback.onComplete(offsets.asJava, null)
      }

      Await.result(done, remainingOrDefault)
      Await.result(control.shutdown(), remainingOrDefault)
    }
  }

  it should "support commit batching" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock, topics = Set("topic1", "topic2"))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msgsTopic1 = (1 to 3).map(createMessage(_, "topic1"))
      val msgsTopic2 = (11 to 13).map(createMessage(_, "topic2"))
      mock.enqueue(msgsTopic1.map(toRecord))
      mock.enqueue(msgsTopic2.map(toRecord))

      probe.request(100)
      val batch = probe.expectNextN(6).map(_.committableOffset)
        .foldLeft(CommittableOffsetBatch.empty) { (b, c) => b.updated(c) }

      val done = batch.commitScaladsl()

      awaitAssert {
        commitLog.calls should have size (1)
      }

      val commitMap = commitLog.calls.head._1
      commitMap(new TopicPartition("topic1", 1)).offset should ===(msgsTopic1.last.record.offset() + 1)
      commitMap(new TopicPartition("topic2", 1)).offset should ===(msgsTopic2.last.record.offset() + 1)

      //emulate commit
      commitLog.calls.map {
        case (offsets, callback) => callback.onComplete(offsets.asJava, null)
      }

      Await.result(done, remainingOrDefault)
      Await.result(control.shutdown(), remainingOrDefault)
    }
  }

  //FIXME looks like current implementation of batch committer is incorrect
  ignore should "support commit batching from more than one stage" in {
    assertAllStagesStopped {
      val commitLog1 = new ConsumerMock.LogHandler()
      val commitLog2 = new ConsumerMock.LogHandler()
      val mock1 = new ConsumerMock[K, V](commitLog1)
      val mock2 = new ConsumerMock[K, V](commitLog2)
      val (control1, probe1) = testSource(mock1, "group1", Set("topic1", "topic2"))
        .toMat(TestSink.probe)(Keep.both)
        .run()
      val (control2, probe2) = testSource(mock2, "group2", Set("topic1", "topic3"))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msgs1a = (1 to 3).map(createMessage(_, "topic1", "group1"))
      val msgs1b = (11 to 13).map(createMessage(_, "topic2", "group1"))
      mock1.enqueue(msgs1a.map(toRecord))
      mock1.enqueue(msgs1b.map(toRecord))

      val msgs2a = (1 to 3).map(createMessage(_, "topic1", "group2"))
      val msgs2b = (11 to 13).map(createMessage(_, "topic3", "group2"))
      mock2.enqueue(msgs2a.map(toRecord))
      mock2.enqueue(msgs2b.map(toRecord))

      probe1.request(100)
      probe2.request(100)

      val batch1 = probe1.expectNextN(6).map(_.committableOffset)
        .foldLeft(CommittableOffsetBatch.empty) { (b, c) => b.updated(c) }

      val batch2 = probe2.expectNextN(6).map(_.committableOffset)
        .foldLeft(batch1) { (b, c) => b.updated(c) }

      val done2 = batch2.commitScaladsl()

      awaitAssert {
        commitLog1.calls should have size (1)
        commitLog2.calls should have size (1)
      }

      val commitMap1 = commitLog1.calls.head._1
      commitMap1(new TopicPartition("topic1", 1)).offset should ===(msgs1a.last.record.offset() + 1)
      commitMap1(new TopicPartition("topic2", 1)).offset should ===(msgs1b.last.record.offset() + 1)

      val commitMap2 = commitLog2.calls.head._1
      commitMap2(new TopicPartition("topic1", 1)).offset should ===(msgs2a.last.record.offset() + 1)
      commitMap2(new TopicPartition("topic3", 1)).offset should ===(msgs2b.last.record.offset() + 1)

      //emulate commit
      commitLog1.calls.map {
        case (offsets, callback) => callback.onComplete(offsets.asJava, null)
      }
      commitLog2.calls.map {
        case (offsets, callback) => callback.onComplete(offsets.asJava, null)
      }

      Await.result(done2, remainingOrDefault)
      Await.result(control1.shutdown(), remainingOrDefault)
      Await.result(control2.shutdown(), remainingOrDefault)
    }
  }

  it should "complete out and keep underlying client open when control.stop called" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      mock.enqueue((1 to 10).map(createMessage).map(toRecord))
      probe.request(1)
      probe.expectNext()

      Await.result(control.stop(), remainingOrDefault)
      probe.expectComplete()

      mock.verifyClosed(never())

      Await.result(control.shutdown(), remainingOrDefault)
      mock.verifyClosed()
    }
  }

  it should "complete stop's Future after stage was shutdown" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe.request(1)
      Await.result(control.stop(), remainingOrDefault)
      probe.expectComplete()

      Await.result(control.shutdown(), remainingOrDefault)
      Await.result(control.stop(), remainingOrDefault)
    }
  }

  it should "return completed Future in stop after shutdown" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe.cancel()
      Await.result(control.isShutdown, remainingOrDefault)
      control.stop().value.get.get shouldBe Done
    }
  }

  it should "be ok to call control.stop multiple times" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      mock.enqueue((1 to 10).map(createMessage).map(toRecord))
      probe.request(1)
      probe.expectNext()

      val stops = (1 to 5).map(_ => control.stop())
      Await.result(Future.sequence(stops), remainingOrDefault)

      probe.expectComplete()
      Await.result(control.shutdown(), remainingOrDefault)
    }
  }

  it should "keep stage running until all futures completed" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msgs = (1 to 10).map(createMessage)
      mock.enqueue(msgs.map(toRecord))

      probe.request(100)
      val done = probe.expectNext().committableOffset.commitScaladsl()
      val rest = probe.expectNextN(9)

      awaitAssert {
        commitLog.calls should have size (1)
      }

      val stopped = control.shutdown()
      probe.expectComplete()

      Thread.sleep(100)
      stopped.isCompleted should ===(false)

      //emulate commit
      commitLog.calls.map {
        case (offsets, callback) => callback.onComplete(offsets.asJava, null)
      }

      Await.result(done, remainingOrDefault)
      Await.result(stopped, remainingOrDefault)
      mock.verifyClosed()
    }
  }

  it should "complete futures with failure when commit after stop" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msg = createMessage(1)
      mock.enqueue(List(toRecord(msg)))

      probe.request(100)
      val first = probe.expectNext()

      val stopped = control.shutdown()
      probe.expectComplete()
      Await.result(stopped, remainingOrDefault)

      val done = first.committableOffset.commitScaladsl()
      intercept[AskTimeoutException] {
        Await.result(done, remainingOrDefault)
      }
    }
  }

  // not implemented yet
  ignore should "keep stage running after cancellation until all futures completed" in {
    assertAllStagesStopped {
      val commitLog = new ConsumerMock.LogHandler()
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = testSource(mock)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val msgs = (1 to 10).map(createMessage)
      mock.enqueue(msgs.map(toRecord))

      probe.request(5)
      val done = probe.expectNext().committableOffset.commitScaladsl()
      val more = probe.expectNextN(4)

      awaitAssert {
        commitLog.calls should have size (1)
      }

      probe.cancel()
      probe.expectNoMsg(200.millis)
      control.isShutdown.isCompleted should ===(false)

      //emulate commit
      commitLog.calls.map {
        case (offsets, callback) => callback.onComplete(offsets.asJava, null)
      }

      Await.result(done, remainingOrDefault)
      Await.result(control.isShutdown, remainingOrDefault)
      mock.verifyClosed()
    }
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
  private var responses = collection.immutable.Queue.empty[Seq[ConsumerRecord[K, V]]]
  private var pendingSubscriptions = List.empty[(List[String], ConsumerRebalanceListener)]
  private var assignment = Set.empty[TopicPartition]
  private var messagesRequested = false
  val mock = {
    val result = Mockito.mock(classOf[KafkaConsumer[K, V]])
    Mockito.when(result.poll(mockito.Matchers.any[Long])).thenAnswer(new Answer[ConsumerRecords[K, V]] {
      override def answer(invocation: InvocationOnMock) = ConsumerMock.this.synchronized {
        pendingSubscriptions.foreach {
          case (topics, callback) =>
            val tps = topics.map { t => new TopicPartition(t, 1) }
            assignment ++= tps
            callback.onPartitionsAssigned(tps.asJavaCollection)
        }
        pendingSubscriptions = List.empty
        val records = if (messagesRequested) {
          responses.dequeueOption.map {
            case (element, remains) =>
              responses = remains
              element
                .groupBy(x => new TopicPartition(x.topic(), x.partition()))
                .map {
                  case (topicPart, messages) => (topicPart, messages.asJava)
                }
          }.getOrElse(Map.empty)
        }
        else Map.empty[TopicPartition, java.util.List[ConsumerRecord[K, V]]]
        new ConsumerRecords[K, V](records.asJava)
      }
    })
    Mockito.when(result.commitAsync(mockito.Matchers.any[JMap[TopicPartition, OffsetAndMetadata]], mockito.Matchers.any[OffsetCommitCallback])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        val offsets = invocation.getArgumentAt(0, classOf[JMap[TopicPartition, OffsetAndMetadata]])
        val callback = invocation.getArgumentAt(1, classOf[OffsetCommitCallback])
        handler(offsets.asScala.toMap, callback)
        ()
      }
    })
    Mockito.when(result.subscribe(mockito.Matchers.any[JList[String]], mockito.Matchers.any[ConsumerRebalanceListener])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        val topics = invocation.getArgumentAt(0, classOf[JList[String]])
        val callback = invocation.getArgumentAt(1, classOf[ConsumerRebalanceListener])
        pendingSubscriptions :+= (topics.asScala.toList -> callback)
        ()
      }
    })
    Mockito.when(result.resume(mockito.Matchers.any[java.util.Collection[TopicPartition]])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        messagesRequested = true
        ()
      }
    })
    Mockito.when(result.pause(mockito.Matchers.any[java.util.Collection[TopicPartition]])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        messagesRequested = false
        ()
      }
    })
    Mockito.when(result.assignment()).thenAnswer(new Answer[JSet[TopicPartition]] {
      override def answer(invocation: InvocationOnMock) = assignment.asJava
    })
    result
  }

  def enqueue(records: Seq[ConsumerRecord[K, V]]) = {
    synchronized {
      responses :+= records
    }
  }

  def verifyClosed(mode: VerificationMode = Mockito.times(1)) = {
    verify(mock, mode).close()
  }

  def verifyPoll(mode: VerificationMode = Mockito.atLeastOnce()) = {
    verify(mock, mode).poll(mockito.Matchers.any[Long])
  }
}

class FailingConsumerMock[K, V](throwable: Throwable, failOnCallNumber: Int*) extends ConsumerMock[K, V] {
  var callNumber = 0

  Mockito.when(mock.poll(mockito.Matchers.any[Long])).thenAnswer(new Answer[ConsumerRecords[K, V]] {
    override def answer(invocation: InvocationOnMock) = FailingConsumerMock.this.synchronized {
      callNumber = callNumber + 1
      if (failOnCallNumber.contains(callNumber))
        throw throwable
      else new ConsumerRecords[K, V](Map.empty[TopicPartition, java.util.List[ConsumerRecord[K, V]]].asJava)
    }
  })
}
