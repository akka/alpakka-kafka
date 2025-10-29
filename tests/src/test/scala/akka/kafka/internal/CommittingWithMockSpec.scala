/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage._
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.tests.scaladsl.LogCapturing
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{
  CoordinatorLoadInProgressException,
  RebalanceInProgressException,
  TimeoutException
}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object CommittingWithMockSpec {
  type K = String
  type V = String
  type Record = ConsumerRecord[K, V]

  def createMessage(seed: Int): CommittableMessage[K, V] = createMessage(seed, "topic")

  def createMessage(seed: Int,
                    topic: String,
                    groupId: String = "group1",
                    metadata: String = ""): CommittableMessage[K, V] = {
    val offset = PartitionOffset(GroupTopicPartition(groupId, topic, 1), seed.toLong)
    val record = new ConsumerRecord(offset.key.topic, offset.key.partition, offset.offset, seed.toString, seed.toString)
    CommittableMessage(record, CommittableOffsetImpl(offset, metadata)(null))
  }

  def toRecord(msg: CommittableMessage[K, V]): ConsumerRecord[K, V] = msg.record
}

class CommittingWithMockSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with Eventually
    with ScalaFutures
    with IntegrationPatience
    with LogCapturing
    with Repeated {

  import CommittingWithMockSpec._

  def this() =
    this(
      ActorSystem("CommittingWithMockSpec",
                  ConfigFactory
                    .parseString("""akka.stream.materializer.debug.fuzzing-mode = on""")
                    .withFallback(ConfigFactory.load()))
    )

  override def afterAll(): Unit =
    shutdown(system)

  implicit val ec: ExecutionContext = _system.dispatcher
  val messages = (1 to 1000).map(createMessage)
  val failure = new CommitFailedException()
  val onCompleteFailure: ConsumerMock.OnCompleteHandler = _ => (null, failure)

  def createCommittableSource(mock: Consumer[K, V],
                              groupId: String = "group1",
                              topics: Set[String] = Set("topic")): Source[CommittableMessage[K, V], Control] =
    Consumer
      .committableSource(
        ConsumerSettings
          .create(system, new StringDeserializer, new StringDeserializer)
          .withGroupId(groupId)
          .withConsumerFactory(_ => mock)
          .withStopTimeout(0.seconds),
        Subscriptions.topics(topics)
      )

  def createSourceWithMetadata(mock: Consumer[K, V],
                               metadataFromRecord: ConsumerRecord[K, V] => String,
                               groupId: String = "group1",
                               topics: Set[String] = Set("topic")): Source[CommittableMessage[K, V], Control] =
    Consumer.commitWithMetadataSource(
      ConsumerSettings
        .create(system, new StringDeserializer, new StringDeserializer)
        .withGroupId(groupId)
        .withCloseTimeout(ConsumerMock.closeTimeout)
        .withConsumerFactory(_ => mock),
      Subscriptions.topics(topics),
      metadataFromRecord
    )

  it should "commit metadata in message" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)

    val (control, probe) = createSourceWithMetadata(mock.mock, (rec: ConsumerRecord[K, V]) => rec.offset.toString)
      .toMat(TestSink())(Keep.both)
      .run()

    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    probe.request(100)
    val done = probe.expectNext().committableOffset.commitInternal()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val (topicPartition, offsetMeta) = commitLog.calls.head._1.head
    topicPartition.topic should ===(msg.record.topic())
    topicPartition.partition should ===(msg.record.partition())
    // committed offset should be the next message the application will consume, i.e. +1
    offsetMeta.offset should ===(msg.record.offset() + 1)
    offsetMeta.metadata should ===(msg.record.offset.toString)

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this, 0L)

    Await.result(done, remainingOrDefault)
    Await.result(control.shutdown(), remainingOrDefault)
  }

  it should "call commitAsync for commit message and then complete future" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = createCommittableSource(mock.mock)
      .toMat(TestSink())(Keep.both)
      .run()

    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    probe.request(100)
    val done = probe.expectNext().committableOffset.commitInternal()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val (topicPartition, offsetMeta) = commitLog.calls.head._1.head
    topicPartition.topic should ===(msg.record.topic())
    topicPartition.partition should ===(msg.record.partition())
    // committed offset should be the next message the application will consume, i.e. +1
    offsetMeta.offset should ===(msg.record.offset() + 1)

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this, 0L)

    Await.result(done, remainingOrDefault)
    Await.result(control.shutdown(), remainingOrDefault)
  }

  it should "fail future in case of commit fail" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler(onCompleteFailure)
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = createCommittableSource(mock.mock)
      .toMat(TestSink())(Keep.both)
      .run()

    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    probe.request(100)
    val done = probe.expectNext().committableOffset.commitInternal()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this)

    intercept[Exception] {
      Await.result(done, remainingOrDefault)
    } should be(failure)
    Await.result(control.shutdown(), remainingOrDefault)
  }

  val exceptions = List(
    new RebalanceInProgressException(),
    new TimeoutException(),
    new CoordinatorLoadInProgressException("coordinator-load-in-progress-exception"),
    new RetriableCommitFailedException(new CommitTimeoutException("injected15"))
  )
  for (exception <- exceptions) {
    it should s"retry commit on ${exception.getClass.getSimpleName}" in assertAllStagesStopped {
      val retries = 4
      val callNo = new AtomicInteger()
      val onCompleteFailure: ConsumerMock.OnCompleteHandler = { offsets =>
        if (callNo.getAndIncrement() < retries) (null, exception)
        else (offsets, null)
      }
      val commitLog = new ConsumerMock.LogHandler(onCompleteFailure)
      val mock = new ConsumerMock[K, V](commitLog)
      val (control, probe) = createCommittableSource(mock.mock)
        .toMat(TestSink())(Keep.both)
        .run()

      val msg = createMessage(1)
      mock.enqueue(List(toRecord(msg)))

      probe.request(100)
      val done = probe.expectNext().committableOffset.commitInternal()

      awaitAssert {
        commitLog.calls should have size (1)
      }

      // allow poll to emulate commits
      mock.releaseAndAwaitCommitCallbacks(this)

      // the first commit and the retries should be captured
      awaitAssert {
        commitLog.calls should have size (retries + 1L)
      }

      done.futureValue shouldBe Done
      control.shutdown().futureValue shouldBe Done
    }
  }

  it should "collect commits to be sent to commitAsync" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = createCommittableSource(mock.mock)
      .toMat(TestSink())(Keep.both)
      .run()

    val count = 100
    val msgs = (1 to count).map(createMessage)
    mock.enqueue(msgs.map(toRecord))

    probe.request(count.toLong)
    val allCommits =
      Future.sequence(probe.expectNextN(count.toLong).map(_.committableOffset.commitInternal()))

    withClue("the commits are aggregated to a low number of calls to commitAsync:") {
      awaitAssert {
        val callsToCommitAsync = commitLog.calls.size
        callsToCommitAsync should be >= 1
        callsToCommitAsync should be < count / 10
      }
    }

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this, count.toLong)

    allCommits.futureValue should have size (count.toLong)
    Await.result(control.shutdown(), remainingOrDefault)
  }

  it should "support commit batching" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = createCommittableSource(mock.mock, topics = Set("topic1", "topic2"))
      .toMat(TestSink())(Keep.both)
      .run()

    val msgsTopic1 = (1 to 3).map(createMessage(_, "topic1"))
    val msgsTopic2 = (11 to 13).map(createMessage(_, "topic2"))
    mock.enqueue(msgsTopic1.map(toRecord))
    mock.enqueue(msgsTopic2.map(toRecord))

    probe.request(100)
    val batch = probe
      .expectNextN(6)
      .map(_.committableOffset)
      .foldLeft(CommittableOffsetBatch.empty)(_.updated(_))

    val done = batch.commitInternal()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val commitMap = commitLog.calls.head._1
    commitMap(new TopicPartition("topic1", 1)).offset should ===(msgsTopic1.last.record.offset() + 1)
    commitMap(new TopicPartition("topic2", 1)).offset should ===(msgsTopic2.last.record.offset() + 1)

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this)

    Await.result(done, remainingOrDefault)
    Await.result(control.shutdown(), remainingOrDefault)
  }

  it should "support commit batching with metadata" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = createSourceWithMetadata(mock.mock,
                                                    (rec: ConsumerRecord[K, V]) => rec.offset.toString,
                                                    topics = Set("topic1", "topic2"))
      .toMat(TestSink())(Keep.both)
      .run()

    val msgsTopic1 = (1 to 3).map(createMessage(_, "topic1"))
    val msgsTopic2 = (11 to 13).map(createMessage(_, "topic2"))
    mock.enqueue(msgsTopic1.map(toRecord))
    mock.enqueue(msgsTopic2.map(toRecord))

    probe.request(100)
    val batch = probe
      .expectNextN(6)
      .map(_.committableOffset)
      .foldLeft(CommittableOffsetBatch.empty)(_.updated(_))

    val done = batch.commitInternal()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val commitMap = commitLog.calls.head._1
    commitMap(new TopicPartition("topic1", 1)).offset should ===(msgsTopic1.last.record.offset() + 1)
    commitMap(new TopicPartition("topic2", 1)).offset should ===(msgsTopic2.last.record.offset() + 1)
    commitMap(new TopicPartition("topic1", 1)).metadata() should ===(msgsTopic1.last.record.offset().toString)
    commitMap(new TopicPartition("topic2", 1)).metadata() should ===(msgsTopic2.last.record.offset().toString)

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this)

    Await.result(done, remainingOrDefault)
    Await.result(control.shutdown(), remainingOrDefault)
  }

  it should "support merging commit batches with metadata" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = createSourceWithMetadata(mock.mock,
                                                    (rec: ConsumerRecord[K, V]) => rec.offset.toString,
                                                    topics = Set("topic1", "topic2"))
      .toMat(TestSink())(Keep.both)
      .run()

    val msgsTopic1 = (1 to 3).map(createMessage(_, "topic1"))
    val msgsTopic2 = (11 to 13).map(createMessage(_, "topic2"))
    mock.enqueue(msgsTopic1.map(toRecord))
    mock.enqueue(msgsTopic2.map(toRecord))

    probe.request(100)
    val batch = probe
      .expectNextN(6)
      .map(_.committableOffset)
      .grouped(2)
      .map(_.foldLeft(CommittableOffsetBatch.empty)(_ updated _))
      .foldLeft(CommittableOffsetBatch.empty)(_ updated _)

    val done = batch.commitInternal()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val commitMap = commitLog.calls.head._1
    commitMap(new TopicPartition("topic1", 1)).offset should ===(msgsTopic1.last.record.offset() + 1)
    commitMap(new TopicPartition("topic2", 1)).offset should ===(msgsTopic2.last.record.offset() + 1)
    commitMap(new TopicPartition("topic1", 1)).metadata() should ===(msgsTopic1.last.record.offset().toString)
    commitMap(new TopicPartition("topic2", 1)).metadata() should ===(msgsTopic2.last.record.offset().toString)

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this)

    Await.result(done, remainingOrDefault)
    Await.result(control.shutdown(), remainingOrDefault)
  }

  //FIXME looks like current implementation of batch committer is incorrect
  it should "support commit batching from more than one stage" in assertAllStagesStopped {
    val commitLog1 = new ConsumerMock.LogHandler()
    val commitLog2 = new ConsumerMock.LogHandler()
    val mock1 = new ConsumerMock[K, V](commitLog1)
    val mock2 = new ConsumerMock[K, V](commitLog2)
    val (control1, probe1) = createCommittableSource(mock1.mock, "group1", Set("topic1", "topic2"))
      .toMat(TestSink())(Keep.both)
      .run()
    val (control2, probe2) = createCommittableSource(mock2.mock, "group2", Set("topic1", "topic3"))
      .toMat(TestSink())(Keep.both)
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

    val batch1 = probe1
      .expectNextN(6)
      .map(_.committableOffset)
      .foldLeft(CommittableOffsetBatch.empty)(_.updated(_))

    val batch2 = probe2
      .expectNextN(6)
      .map(_.committableOffset)
      .foldLeft(batch1)(_.updated(_))

    val done2 = batch2.commitInternal()

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

    // allow poll to emulate commits
    mock1.releaseAndAwaitCommitCallbacks(this)
    mock2.releaseAndAwaitCommitCallbacks(this)

    Await.result(done2, remainingOrDefault)
    Await.result(control1.shutdown(), remainingOrDefault)
    Await.result(control2.shutdown(), remainingOrDefault)
  }

  // Same logic as "support commit batching with metadata" above
  "Tell committing" should "support commit batching with metadata" in assertAllStagesStopped {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, probe) = createSourceWithMetadata(mock.mock,
                                                    (rec: ConsumerRecord[K, V]) => rec.offset.toString,
                                                    topics = Set("topic1", "topic2"))
      .toMat(TestSink())(Keep.both)
      .run()

    val msgsTopic1 = (1 to 3).map(createMessage(_, "topic1"))
    val msgsTopic2 = (11 to 13).map(createMessage(_, "topic2"))
    mock.enqueue(msgsTopic1.map(toRecord))
    mock.enqueue(msgsTopic2.map(toRecord))

    probe.request(100)
    val batch = probe
      .expectNextN(6)
      .map(_.committableOffset)
      .foldLeft(CommittableOffsetBatch.empty)(_.updated(_))

    batch.tellCommit()

    awaitAssert {
      commitLog.calls should have size (1)
    }

    val commitMap = commitLog.calls.head._1
    commitMap(new TopicPartition("topic1", 1)).offset should ===(msgsTopic1.last.record.offset() + 1)
    commitMap(new TopicPartition("topic2", 1)).offset should ===(msgsTopic2.last.record.offset() + 1)
    commitMap(new TopicPartition("topic1", 1)).metadata() should ===(msgsTopic1.last.record.offset().toString)
    commitMap(new TopicPartition("topic2", 1)).metadata() should ===(msgsTopic2.last.record.offset().toString)

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this)

    Await.result(control.shutdown(), remainingOrDefault)
  }

  "Committer.flow" should "fail in case of an exception during commit" in assertAllStagesStopped {
    val committerSettings = CommitterSettings(system)
      .withMaxBatch(1L)

    val commitLog = new internal.ConsumerMock.LogHandler(onCompleteFailure)
    val mock = new ConsumerMock[K, V](commitLog)
    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    val (control, probe) = createCommittableSource(mock.mock)
      .map(_.committableOffset)
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .run()

    awaitAssert {
      commitLog.calls should have size 1
    }

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this)

    probe.failed.futureValue shouldBe a[CommitFailedException]
    control.shutdown().futureValue shouldBe Done
  }

  it should "recover with supervision in case of commit fail" in assertAllStagesStopped {
    val committerSettings = CommitterSettings(system)
      .withMaxBatch(1L)

    val commitLog = new ConsumerMock.LogHandler(onCompleteFailure)
    val mock = new ConsumerMock[K, V](commitLog)
    val msg = createMessage(1)
    mock.enqueue(List(toRecord(msg)))

    val resumeOnCommitFailed: Supervision.Decider = {
      case _: CommitFailedException => Supervision.Resume
      case _ => Supervision.Stop
    }

    val (control, probe) = createCommittableSource(mock.mock)
      .map(_.committableOffset)
      .toMat(
        Committer
          .sink(committerSettings)
          .withAttributes(ActorAttributes.supervisionStrategy(resumeOnCommitFailed))
      )(Keep.both)
      .run()

    awaitAssert {
      commitLog.calls should have size 1
    }

    // allow poll to emulate commits
    mock.releaseAndAwaitCommitCallbacks(this)

    control.shutdown().futureValue shouldBe Done
    probe.futureValue shouldBe Done
  }
}
