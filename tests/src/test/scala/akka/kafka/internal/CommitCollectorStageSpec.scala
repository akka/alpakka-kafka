/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka.{CommitWhen, CommitterSettings, Repeated}
import akka.kafka.ConsumerMessage.{Committable, CommittableOffset, CommittableOffsetBatch, PartitionOffset}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.testkit.ConsumerResultFactory
import akka.kafka.testkit.scaladsl.{ConsumerControlFactory, Slf4jToAkkaLoggingAdapter}
import akka.kafka.tests.scaladsl.LogCapturing
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.testkit.TestKit
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{AppendedClues, BeforeAndAfterAll, Matchers, WordSpecLike}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}

class CommitCollectorStageSpec(_system: ActorSystem)
    extends TestKit(_system)
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with Eventually
    with IntegrationPatience
    with AppendedClues
    with ScalaFutures
    with Repeated
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

  "The CommitCollectorStage" when {
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

      "emit immediately if there is pending demand" in assertAllStagesStopped {
        val settings = DefaultCommitterSettings.withMaxBatch(Integer.MAX_VALUE).withMaxInterval(100.millis)
        val (sourceProbe, control, sinkProbe, factory) = streamProbesWithOffsetFactory(settings)

        sinkProbe.request(1)
        // the interval triggers, but there is nothing to emit
        sinkProbe.expectNoMessage(200.millis)

        // next trigger should emit this single value immediately
        val msg = factory.makeOffset()
        sourceProbe.sendNext(msg)
        val committedBatch = sinkProbe.expectNext(50.millis)

        committedBatch.batchSize shouldBe 1
        committedBatch.offsets.values should have size 1
        committedBatch.offsets.values.last shouldBe msg.partitionOffset.offset
        factory.committer.commits.size shouldBe 1 withClue "expected only one batch commit"

        control.shutdown().futureValue shouldBe Done
      }

      "emit after a triggered batch when the next batch is full" in assertAllStagesStopped {
        val settings = DefaultCommitterSettings.withMaxBatch(2).withMaxInterval(50.millis)
        val (sourceProbe, control, sinkProbe, factory) = streamProbesWithOffsetFactory(settings)

        val msg = factory.makeOffset()
        sourceProbe.sendNext(msg)
        // triggered by interval
        val committedBatch = sinkProbe.requestNext(80.millis)

        val msg2 = factory.makeOffset()
        sourceProbe.sendNext(msg2)
        val msg3 = factory.makeOffset()
        sourceProbe.sendNext(msg3)

        // triggered by size
        val committedBatch2 = sinkProbe.requestNext(10.millis)

        committedBatch.batchSize shouldBe 1
        committedBatch.offsets.values should have size 1
        committedBatch.offsets.values.last shouldBe msg.partitionOffset.offset

        committedBatch2.batchSize shouldBe 2
        committedBatch2.offsets.values should have size 1
        committedBatch2.offsets.values.last shouldBe msg3.partitionOffset.offset

        control.shutdown().futureValue shouldBe Done
      }
    }

    "all offsets are in batch that is in flight" should {
      val settings =
        DefaultCommitterSettings.withMaxBatch(Integer.MAX_VALUE).withMaxInterval(10.hours).withParallelism(1)

      "batch commit all buffered elements if upstream has suddenly completed" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe, factory) = streamProbesWithOffsetFactory(settings)

        sinkProbe.ensureSubscription()
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
        val settings = // special config to have more than one batch before failure
          DefaultCommitterSettings.withMaxBatch(3).withMaxInterval(10.hours).withParallelism(100)

        val (sourceProbe, control, sinkProbe, factory) = streamProbesWithOffsetFactory(settings)

        sinkProbe.request(100)

        val msgs = (1 to 10).map(_ => factory.makeOffset())

        msgs.foreach(sourceProbe.sendNext)

        val testError = new IllegalStateException("BOOM")
        sourceProbe.sendError(testError)

        val receivedError = pullTillFailure(sinkProbe, maxEvents = 4)

        receivedError shouldBe testError

        val commits = factory.committer.commits

        commits.last.offset shouldBe 10 withClue "last offset commit should be exactly the one preceeding the error"

        control.shutdown().futureValue shouldBe Done
      }
    }

    "using next observed offset" should {
      val settings = DefaultCommitterSettings.withMaxBatch(1).withCommitWhen(CommitWhen.NextOffsetObserved)
      "only commit when the next offset is observed" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe) = streamProbes(settings)
        val committer = new TestBatchCommitter(settings)
        val offsetFactory = TestOffsetFactory(committer)
        val (msg1, msg2, msg3) = (offsetFactory.makeOffset(), offsetFactory.makeOffset(), offsetFactory.makeOffset())

        sinkProbe.request(100)

        // first message should not be committed but 'batched-up'
        sourceProbe.sendNext(msg1)
        sourceProbe.sendNext(msg2)
        sourceProbe.sendNext(msg3)

        val batches = sinkProbe.expectNextN(2)
        sinkProbe.expectNoMessage(10.millis)

        // batches are committed using mapAsyncUnordered, so it's possible to receive batch acknowledgements
        // downstream out of order
        val lastBatch = batches.maxBy(_.offsets.values.last)

        lastBatch.offsets.values.last shouldBe msg2.partitionOffset.offset withClue "expect only the second offset to be committed"
        committer.commits.size shouldBe 2 withClue "expected only two commits"

        control.shutdown().futureValue shouldBe Done
      }
      "only commit when the next offset is observed in a CommittableOffsetBatch" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe, offsetFactory) = streamProbesWithOffsetFactory(settings)
        // create batches of size 1
        val (batch1, batch2, batch3) =
          (offsetFactory.makeBatchOffset(), offsetFactory.makeBatchOffset(), offsetFactory.makeBatchOffset())

        sinkProbe.request(100)

        // commit in first batch should not be committed but 'batched-up' again
        sourceProbe.sendNext(batch1)
        sourceProbe.sendNext(batch2)
        sourceProbe.sendNext(batch3)

        val batches = sinkProbe.expectNextN(2)
        sinkProbe.expectNoMessage(10.millis)

        // batches are committed using mapAsyncUnordered, so it's possible to receive batch acknowledgements
        // downstream out of order
        val lastBatch = batches.maxBy(_.offsets.values.last)

        lastBatch.offsets.values.last shouldBe batch2
          .asInstanceOf[CommittableOffsetBatch]
          .offsets
          .head
          ._2 withClue "expect only the second offset to be committed"
        offsetFactory.committer.commits.size shouldBe 2 withClue "expected only two commits"

        control.shutdown().futureValue shouldBe Done
      }
      "only commit when the next offset is observed in a CommittableOffset preceded by a CommittableOffsetBatch" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe, offsetFactory) = streamProbesWithOffsetFactory(settings)
        // create a mix of single offsets and batches of 1
        val (batch1, msg2, batch3) =
          (offsetFactory.makeBatchOffset(), offsetFactory.makeOffset(), offsetFactory.makeBatchOffset())

        sinkProbe.request(100)

        // first batch should not be committed but 'batched-up' again
        sourceProbe.sendNext(batch1)
        sourceProbe.sendNext(msg2)
        sourceProbe.sendNext(batch3)

        val batches = sinkProbe.expectNextN(2)
        sinkProbe.expectNoMessage(10.millis)

        // batches are committed using mapAsyncUnordered, so it's possible to receive batch acknowledgements
        // downstream out of order
        val lastBatch = batches.maxBy(_.offsets.values.last)

        lastBatch.offsets.values.last shouldBe msg2.partitionOffset.offset withClue "expect only the second offset to be committed"
        offsetFactory.committer.commits.size shouldBe 2 withClue "expected only two commits"

        control.shutdown().futureValue shouldBe Done
      }
      "only commit when the next offset is observed in a CommittableOffsetBatch preceded by a CommittableOffset" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe, offsetFactory) = streamProbesWithOffsetFactory(settings)
        // create a mix of single offsets and batches of 1
        val (msg1, batch2, msg3) =
          (offsetFactory.makeOffset(), offsetFactory.makeBatchOffset(), offsetFactory.makeOffset())

        sinkProbe.request(100)

        // first batch should not be committed but 'batched-up' again
        sourceProbe.sendNext(msg1)
        sourceProbe.sendNext(batch2)
        sourceProbe.sendNext(msg3)

        val batches = sinkProbe.expectNextN(2)
        sinkProbe.expectNoMessage(10.millis)

        // batches are committed using mapAsyncUnordered, so it's possible to receive batch acknowledgements
        // downstream out of order
        val lastBatch = batches.maxBy(_.offsets.values.last)

        lastBatch.offsets.values.last shouldBe batch2
          .asInstanceOf[CommittableOffsetBatch]
          .offsets
          .head
          ._2 withClue "expect only the second offset to be committed"
        offsetFactory.committer.commits.size shouldBe 2 withClue "expected only two commits"

        control.shutdown().futureValue shouldBe Done
      }
      "only commit when the next offset is observed for the correct partitions" in assertAllStagesStopped {
        val (sourceProbe, control, sinkProbe) = streamProbes(settings)
        val committer = new TestBatchCommitter(settings)
        val offsetFactory = TestOffsetFactory(committer)
        val (msg1, msg2, msg3, msg4, msg5) = (offsetFactory.makeOffset(partitionNum = 1),
                                              offsetFactory.makeOffset(partitionNum = 2),
                                              offsetFactory.makeOffset(partitionNum = 1),
                                              offsetFactory.makeOffset(partitionNum = 2),
                                              offsetFactory.makeOffset(partitionNum = 1))
        val all = Seq(msg1, msg2, msg3, msg4, msg5)

        sinkProbe.request(100)
        all.foreach(sourceProbe.sendNext)
        val batches = sinkProbe.expectNextN(3)
        sinkProbe.expectNoMessage(10.millis)

        // batches are committed using mapAsyncUnordered, so it's possible to receive batch acknowledgements
        // downstream out of order.  get the last 2 batches.
        val lastBatch :: secondLastBatch :: Nil = batches.sortBy(_.offsets.values.last).takeRight(2).reverse

        lastBatch.offsets(msg3.partitionOffset.key) shouldBe msg3.partitionOffset.offset withClue "expect the second offset of partition 1"
        secondLastBatch.offsets(msg2.partitionOffset.key) shouldBe msg2.partitionOffset.offset withClue "expect the first offset of partition 2"
        committer.commits.size shouldBe 3 withClue "expected only three commits"

        control.shutdown().futureValue shouldBe Done
      }
    }
  }

  @scala.annotation.tailrec
  private def pullTillFailure(
      sinkProbe: TestSubscriber.Probe[CommittableOffsetBatch],
      maxEvents: Int
  ): Throwable = {
    val nextOrError = sinkProbe.expectNextOrError()
    if (maxEvents < 0) {
      fail("Max number events has been read, no error encountered.")
    }
    nextOrError match {
      case Left(ex) =>
        log.debug("Received failure")
        ex
      case Right(_) =>
        log.debug("Received batch {}")
        pullTillFailure(sinkProbe, maxEvents - 1)
    }
  }

  private def streamProbes(
      committerSettings: CommitterSettings
  ): (TestPublisher.Probe[Committable], Consumer.Control, TestSubscriber.Probe[CommittableOffsetBatch]) = {

    val flow = Committer.batchFlow(committerSettings)

    val ((source, control), sink) = TestSource
      .probe[Committable]
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.both)
      .via(flow)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    (source, control, sink)
  }

  private def streamProbesWithOffsetFactory(
      committerSettings: CommitterSettings
  ): (TestPublisher.Probe[Committable],
      Consumer.Control,
      TestSubscriber.Probe[CommittableOffsetBatch],
      TestOffsetFactory) = {
    val (source, control, sink) = streamProbes(committerSettings)
    val factory = TestOffsetFactory(new TestBatchCommitter(committerSettings))
    (source, control, sink, factory)
  }

  object TestCommittableOffset {

    def apply(offsetCounter: AtomicLong,
              committer: TestBatchCommitter,
              failWith: Option[Throwable] = None,
              partitionNum: Int = 1): CommittableOffset = {
      CommittableOffsetImpl(
        ConsumerResultFactory
          .partitionOffset(groupId = "group1",
                           topic = "topic1",
                           partition = partitionNum,
                           offset = offsetCounter.incrementAndGet()),
        "metadata1"
      )(committer.underlying)
    }
  }

  class TestOffsetFactory(val committer: TestBatchCommitter) {
    private val offsetCounter = new AtomicLong(0L)

    def makeOffset(failWith: Option[Throwable] = None, partitionNum: Int = 1): CommittableOffset = {
      TestCommittableOffset(offsetCounter, committer, failWith, partitionNum)
    }

    def makeBatchOffset(failWith: Option[Throwable] = None, partitionNum: Int = 1): CommittableOffsetBatch = {
      CommittableOffsetBatch(makeOffset(failWith, partitionNum))
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
      implicit system: ActorSystem
  ) {

    var commits = List.empty[PartitionOffset]

    private def completeCommit(): Future[Done] = {
      val promisedCommit = Promise[Done]
      system.scheduler.scheduleOnce(commitDelay()) {
        promisedCommit.success(Done)
      }
      promisedCommit.future
    }

    private[akka] val underlying =
      new KafkaAsyncConsumerCommitterRef(consumerActor = null, commitSettings.maxInterval) {
        override def commitSingle(offset: CommittableOffsetImpl): Future[Done] = {
          commits = commits :+ offset.partitionOffset
          completeCommit()
        }

        override def commit(batch: CommittableOffsetBatch): Future[Done] = {
          val offsets = batch.offsets.toList.map { case (partition, offset) => PartitionOffset(partition, offset) }
          commits = commits ++ offsets
          completeCommit()
        }

        override def tellCommit(batch: CommittableOffsetBatch, emergency: Boolean): Unit = commit(batch)
      }
  }
}
