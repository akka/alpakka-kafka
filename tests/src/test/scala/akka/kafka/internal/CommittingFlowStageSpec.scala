/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch, PartitionOffset}
import akka.kafka.scaladsl.Consumer
import akka.kafka.testkit.ConsumerResultFactory
import akka.kafka.testkit.scaladsl.{ConsumerControlFactory, Slf4jToAkkaLoggingAdapter}
import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka.{CommitterSettings, ConsumerMessage}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.testkit.TestKit
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{AppendedClues, BeforeAndAfterAll, Matchers, WordSpecLike}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}

class CommittingFlowStageSpec(_system: ActorSystem)
    extends TestKit(_system)
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with Eventually
    with IntegrationPatience
    with AppendedClues
    with ScalaFutures
    with LogCapturing {

  implicit lazy val materializer: ActorMaterializer = ActorMaterializer()
  implicit lazy val executionContext: ExecutionContext = system.dispatcher

  val DefaultCommitterSettings: CommitterSettings = CommitterSettings(system)
  val msgAbsenceDuration: FiniteDuration = 2.seconds

  val log: Logger = LoggerFactory.getLogger(getClass)
  // used by the .log(...) stream operator
  implicit val adapter: LoggingAdapter = new Slf4jToAkkaLoggingAdapter(log)

  def this() = this(ActorSystem())

  override def afterAll(): Unit = shutdown(system)

  "The CommittingFlowStage" when {
    "the batch is full" should {
      val settings = DefaultCommitterSettings.withMaxBatch(2).withMaxInterval(10.hours)
      "batch commit without errors" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe) = streamProbes(settings)
        val committer = new TestBatchCommitter(settings)
        val offsetFactory = TestOffsetFactory(committer)
        val (msg1, msg2) = (offsetFactory.makeOffset(), offsetFactory.makeOffset())

        sinkProbe.request(100)

        // first message should not be committed but 'batched-up'
        sourceProbe.sendNext(msg1)
        sinkProbe.expectNoMessage(msgAbsenceDuration)
        committer.commits shouldBe empty

        // now message that fills up the batch
        sourceProbe.sendNext(msg2)

        val committedBatch = sinkProbe.expectNext()

        committedBatch.batchSize shouldBe 2
        committedBatch.offsets.values should have size 1
        committedBatch.offsets.values.last shouldBe msg2.partitionOffset.offset
        committer.commits.size shouldBe 1 withClue "expected only one batch commit"

        control.shutdown().futureValue shouldBe Done
      }
    }

    "batch duration has elapsed" should {
      val settings = DefaultCommitterSettings.withMaxBatch(Integer.MAX_VALUE).withMaxInterval(1.milli)
      "batch commit without errors" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe, factory) = streamProbesWithOffsetFactory(settings)

        sinkProbe.request(100)

        val msg = factory.makeOffset()

        sourceProbe.sendNext(msg)
        val committedBatch = sinkProbe.expectNext()

        committedBatch.batchSize shouldBe 1
        committedBatch.offsets.values should have size 1
        committedBatch.offsets.values.last shouldBe msg.partitionOffset.offset
        factory.committer.commits.size shouldBe 1 withClue "expected only one batch commit"

        control.shutdown().futureValue shouldBe Done
      }
    }

    "all offsets are in batch that is in flight" should {
      val settings = DefaultCommitterSettings.withMaxBatch(Integer.MAX_VALUE).withMaxInterval(10.hours)

      "batch commit all buffered elements if upstream has suddenly completed" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe, factory) = streamProbesWithOffsetFactory(settings)

        sinkProbe.request(100)

        val msg = factory.makeOffset()
        sourceProbe.sendNext(msg)
        sourceProbe.sendComplete()

        val committedBatch = sinkProbe.expectNext()

        committedBatch.batchSize shouldBe 1
        committedBatch.offsets.values should have size 1
        committedBatch.offsets.values.last shouldBe msg.partitionOffset.offset
        factory.committer.commits.size shouldBe 1 withClue "expected only one batch commit"

        control.shutdown().futureValue shouldBe Done
      }

      "batch commit all buffered elements if upstream has suddenly completed with delayed commits" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe) = streamProbes(settings)
        val committer = new TestBatchCommitter(settings, () => 50.millis)

        val factory = TestOffsetFactory(committer)
        sinkProbe.request(100)

        val (msg1, msg2) = (factory.makeOffset(), factory.makeOffset())
        sourceProbe.sendNext(msg1)
        sourceProbe.sendNext(msg2)
        sourceProbe.sendComplete()

        val committedBatch = sinkProbe.expectNext()

        committedBatch.batchSize shouldBe 2
        committedBatch.offsets.values should have size 1
        committedBatch.offsets.values.last shouldBe msg2.partitionOffset.offset
        committer.commits.size shouldBe 1 withClue "expected only one batch commit"

        control.shutdown().futureValue shouldBe Done
      }

      "batch commit all buffered elements if upstream has suddenly failed" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe, factory) = streamProbesWithOffsetFactory(settings)
        val testError = new IllegalStateException("BOOM")

        sinkProbe.request(100)

        val msg = factory.makeOffset()
        sourceProbe.sendNext(msg)
        sourceProbe.sendError(testError)

        val committedBatch = sinkProbe.expectNext()
        committedBatch.batchSize shouldBe 1
        committedBatch.offsets.values should have size 1
        committedBatch.offsets.values.last shouldBe msg.partitionOffset.offset
        factory.committer.commits.size shouldBe 1 withClue "expected only one batch commit"

        sinkProbe.expectError(testError)

        control.shutdown().futureValue shouldBe Done
      }

    }

  }

  private def streamProbes(
      committerSettings: CommitterSettings
  ): (TestPublisher.Probe[CommittableOffset], Consumer.Control, TestSubscriber.Probe[CommittableOffsetBatch]) = {

    val flow = Flow.fromGraph(new CommittingFlowStage(committerSettings))

    val ((source, control), sink) = TestSource
      .probe[CommittableOffset]
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.both)
      .via(flow)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    (source, control, sink)
  }

  private def streamProbesWithOffsetFactory(
      committerSettings: CommitterSettings
  ): (TestPublisher.Probe[CommittableOffset],
      Consumer.Control,
      TestSubscriber.Probe[CommittableOffsetBatch],
      TestOffsetFactory) = {
    val (source, control, sink) = streamProbes(committerSettings)
    val factory = TestOffsetFactory(new TestBatchCommitter(committerSettings))
    (source, control, sink, factory)
  }
}

object TestCommittableOffset {
  private val offsetCounter = new AtomicLong(0L)

  def apply(committer: TestBatchCommitter, failWith: Option[Throwable] = None): CommittableOffset = {
    CommittableOffsetImpl(
      ConsumerResultFactory
        .partitionOffset(groupId = "group1", topic = "topic1", partition = 1, offset = offsetCounter.incrementAndGet()),
      "metadata1"
    )(committer.underlying)
  }
}

class TestOffsetFactory(val committer: TestBatchCommitter) {
  def makeOffset(failWith: Option[Throwable] = None): CommittableOffset = {
    TestCommittableOffset(committer, failWith)
  }
}

object TestOffsetFactory {

  def apply(committer: TestBatchCommitter): TestOffsetFactory =
    new TestOffsetFactory(committer)
}

class TestBatchCommitter(
    commitSettings: CommitterSettings,
    commitDelay: () => FiniteDuration = () => Duration.Zero
)(
    implicit ec: ExecutionContext,
    system: ActorSystem
) {

  var commits = List.empty[PartitionOffset]

  private def completeCommit(): Future[Done] = {
    val promisedCommit = Promise[Done]
    system.scheduler.scheduleOnce(commitDelay()) {
      promisedCommit.success(Done)
    }
    promisedCommit.future
  }

  private[akka] val underlying = new KafkaAsyncConsumerCommitterRef(consumerActor = null, commitSettings.maxInterval) {
    override def commitSingle(offset: CommittableOffsetImpl, flush: Boolean): Future[Done] = {
      commits = commits :+ offset.partitionOffset
      completeCommit()
    }

    override def commit(batch: ConsumerMessage.CommittableOffsetBatch, flush: Boolean): Future[Done] = {
      val offsets = batch.offsets.toList.map { case (partition, offset) => PartitionOffset(partition, offset) }
      commits = commits ++ offsets
      completeCommit()
    }
  }
}
