/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.kafka.ProducerMessage._
import akka.kafka.{CommitDelivery, CommitterSettings, ProducerSettings}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.stage._
import akka.stream.{Attributes, Inlet, SinkShape, Supervision}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API.
 *
 * Combined stage for producing, batching commits and committing.
 */
@InternalApi
private[kafka] final class CommittingProducerSinkStage[K, V, IN <: Envelope[K, V, Committable]](
    val producerSettings: ProducerSettings[K, V],
    val committerSettings: CommitterSettings
) extends GraphStageWithMaterializedValue[SinkShape[IN], Future[Done]] {

  require(committerSettings.delivery == CommitDelivery.WaitForAck, "only CommitDelivery.WaitForAck may be used")

  val in = Inlet[IN]("messages")
  val shape: SinkShape[IN] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val logic = new CommittingProducerSinkStageLogic(this, inheritedAttributes)
    (logic, logic.streamCompletion.future)
  }
}

private final class CommittingProducerSinkStageLogic[K, V, IN <: Envelope[K, V, Committable]](
    stage: CommittingProducerSinkStage[K, V, IN],
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with CommitObservationLogic
    with StageIdLogging
    with DeferredProducer[K, V] {

  import CommitTrigger._

  /** The promise behind the materialized future. */
  final val streamCompletion = Promise[Done]()

  final val settings: CommitterSettings = stage.committerSettings

  private lazy val decider: Decider =
    inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

  override protected def logSource: Class[_] = classOf[CommittingProducerSinkStage[_, _, _]]

  override protected val producerSettings: ProducerSettings[K, V] = stage.producerSettings

  override protected val closeAndFailStageCb: AsyncCallback[Throwable] = getAsyncCallback[Throwable](closeAndFailStage)

  private def closeAndFailStage(ex: Throwable): Unit = {
    closeProducerImmediately()
    failStage(ex)
    streamCompletion.failure(ex)
  }

  // ---- initialization
  override def preStart(): Unit = {
    super.preStart()
    resolveProducer(stage.producerSettings)
  }

  /** When the producer is set up, the sink pulls and schedules the first commit. */
  override protected def producerAssigned(): Unit = {
    tryPull(stage.in)
    scheduleCommit()
    log.debug("CommittingProducerSink initialized")
  }

  // ---- Producing
  /** Counter for number of outstanding messages that are sent, but didn't get the callback, yet. */
  private var awaitingProduceResult = 0L

  /** Counter for number of outstanding messages that are sent, but the commit did not finish, yet. */
  private var awaitingCommitResult = 0L

  private def produce(in: Envelope[K, V, Committable]): Unit =
    in match {
      case msg: Message[K, V, Committable] =>
        awaitingProduceResult += 1
        awaitingCommitResult += 1
        producer.send(msg.record, new SendCallback(msg.passThrough))

      case multiMessage: MultiMessage[K, V, Committable] if multiMessage.records.isEmpty =>
        awaitingCommitResult += 1
        collectOffset(multiMessage.passThrough)

      case multiMsg: MultiMessage[K, V, Committable] =>
        val size = multiMsg.records.size
        awaitingProduceResult += size
        awaitingCommitResult += 1
        val cb = new SendMultiCallback(size, multiMsg.passThrough)
        for {
          record <- multiMsg.records
        } producer.send(record, cb)

      case msg: PassThroughMessage[K, V, Committable] =>
        awaitingCommitResult += 1
        collectOffset(msg.passThrough)
    }

  private val sendFailureCb: AsyncCallback[(Int, Throwable)] = getAsyncCallback[(Int, Throwable)] {
    case (count, exception) =>
      decider(exception) match {
        case Supervision.Stop => closeAndFailStage(exception)
        case _ => collectOffsetIgnore(count, exception)
      }
  }

  /** send-callback for a single message. */
  private final class SendCallback(offset: Committable) extends Callback {

    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
      if (exception == null) collectOffsetCb.invoke(offset)
      else sendFailureCb.invoke(1 -> exception)
  }

  /** send-callback for a multi-message. */
  private final class SendMultiCallback(count: Int, offset: Committable) extends Callback {
    private val counter = new AtomicInteger(count)

    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
      if (exception == null) {
        if (counter.decrementAndGet() == 0) collectOffsetMultiCb.invoke(count -> offset)
      } else sendFailureCb.invoke(count -> exception)
  }

  // ---- Committing
  private val collectOffsetCb: AsyncCallback[Committable] = getAsyncCallback[Committable] { offset =>
    awaitingProduceResult -= 1
    collectOffset(offset)
  }

  private val collectOffsetMultiCb: AsyncCallback[(Int, Committable)] = getAsyncCallback[(Int, Committable)] {
    case (count, offset) =>
      awaitingProduceResult -= count
      collectOffset(offset)
  }

  private def collectOffsetIgnore(count: Int, exception: Throwable): Unit = {
    log.warning("ignoring send failure {}", exception)
    awaitingCommitResult -= 1
    awaitingProduceResult -= count
  }

  private def scheduleCommit(): Unit =
    scheduleOnce(CommittingProducerSinkStage.CommitNow, stage.committerSettings.maxInterval)

  override protected def onTimer(timerKey: Any): Unit = timerKey match {
    case CommittingProducerSinkStage.CommitNow => commit(Interval)
  }

  private def collectOffset(offset: Committable): Unit =
    if (updateBatch(offset)) commit(BatchSize)
    else if (isClosed(stage.in) && awaitingProduceResult == 0L) commit(UpstreamClosed)

  private def commit(triggeredBy: TriggerdBy): Unit = {
    if (offsetBatch.batchSize != 0) {
      log.debug("commit triggered by {} (awaitingProduceResult={} awaitingCommitResult={})",
                triggeredBy,
                awaitingProduceResult,
                awaitingCommitResult)
      val batchSize = offsetBatch.batchSize
      offsetBatch
        .commitInternal()
        .onComplete(t => commitResultCB.invoke(batchSize -> t))(materializer.executionContext)
      offsetBatch = CommittableOffsetBatch.empty
    }
    scheduleCommit()
  }

  private val commitResultCB: AsyncCallback[(Long, Try[Done])] = getAsyncCallback[(Long, Try[Done])] {
    case (batchSize, Success(_)) =>
      awaitingCommitResult -= batchSize
      checkForCompletion()
    case (batchSize, Failure(exception)) =>
      awaitingCommitResult -= batchSize
      decider(exception) match {
        case Supervision.Stop =>
          log.error("committing failed with {}", exception)
          closeAndFailStage(exception)
        case _ =>
          log.warning("ignored commit failure {}", exception)
      }
      checkForCompletion()
  }

  private def emergencyShutdown(ex: Throwable): Unit = {
    log.debug("Emergency shutdown triggered by {} (awaitingProduceResult={} awaitingCommitResult={})",
              ex,
              awaitingProduceResult,
              awaitingCommitResult)

    offsetBatch.tellCommitEmergency()
    upstreamCompletionState = Some(Failure(ex))
    offsetBatch = CommittableOffsetBatch.empty
    closeAndFailStage(ex)
  }

  // ---- handler and completion
  /** Keeps track of upstream completion signals until this stage shuts down. */
  private var upstreamCompletionState: Option[Try[Done]] = None

  setHandler(
    stage.in,
    new InHandler {
      override def onPush(): Unit = {
        produce(grab(stage.in))
        tryPull(stage.in)
      }

      override def onUpstreamFinish(): Unit =
        if (awaitingCommitsBeforeShutdown()) {
          completeStage()
          streamCompletion.success(Done)
        } else {
          commit(UpstreamFinish)
          setKeepGoing(true)
          upstreamCompletionState = Some(Success(Done))
        }

      override def onUpstreamFailure(ex: Throwable): Unit =
        if (awaitingCommitsBeforeShutdown()) {
          closeAndFailStage(ex)
        } else {
          emergencyShutdown(ex)
        }
    }
  )

  private def awaitingCommitsBeforeShutdown(): Boolean = {
    awaitingCommitResult -= clearDeferredOffsets()
    awaitingCommitResult == 0
  }

  private def checkForCompletion(): Unit =
    if (isClosed(stage.in))
      if (awaitingCommitsBeforeShutdown()) {
        upstreamCompletionState match {
          case Some(Success(_)) =>
            completeStage()
            streamCompletion.success(Done)
          case Some(Failure(ex)) =>
            closeAndFailStage(ex)
          case None =>
            closeAndFailStage(new IllegalStateException("Stage completed, but there is no info about status"))
        }
      } else
        log.debug("checkForCompletion awaitingProduceResult={} awaitingCommitResult={}",
                  awaitingProduceResult,
                  awaitingCommitResult)

  override def postStop(): Unit = {
    log.debug("CommittingProducerSink stopped")
    closeProducer()
    super.postStop()
  }

}

private object CommittingProducerSinkStage {
  val CommitNow = "commit"
}
