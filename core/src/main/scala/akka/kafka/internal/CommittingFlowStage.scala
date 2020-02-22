/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.stage._
import akka.stream._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API.
 *
 * Combined stage for producing, batching commits and committing.
 */
@InternalApi
private[kafka] final class CommittingFlowStage(val committerSettings: CommitterSettings)
    extends GraphStageWithMaterializedValue[FlowShape[Committable, CommittableOffsetBatch], Future[
      CommittableOffsetBatch
    ]] {

  val in: Inlet[Committable] = Inlet[Committable]("FlowIn")
  val out: Outlet[CommittableOffsetBatch] = Outlet[CommittableOffsetBatch]("FlowOut")
  val shape: FlowShape[Committable, CommittableOffsetBatch] = FlowShape(in, out)

  def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[CommittableOffsetBatch]) = {
    val logic = new CommittingFlowStageLogic(this, inheritedAttributes)
    (logic, logic.streamCompletion.future)
  }
}

private final class CommittingFlowStageLogic(
    stage: CommittingFlowStage,
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with StageIdLogging {

  import CommitTrigger._
  import CommittingFlowStage._

  /** The promise behind the materialized future. */
  final val streamCompletion = Promise[CommittableOffsetBatch]

  private lazy val decider: Decider =
    inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

  override protected def logSource: Class[_] = classOf[CommittingFlowStageLogic]

  private def closeAndFailStage(ex: Throwable): Unit = {
    failStage(ex)
    streamCompletion.tryFailure(ex)
  }

  // ---- initialization
  override def preStart(): Unit = {
    super.preStart()
    tryPull(stage.in)
    scheduleCommit()
    log.debug("CommittingFlowStage initialized")
  }

  /** Batches offsets until a commit is triggered. */
  private var offsetBatch: CommittableOffsetBatch = CommittableOffsetBatch.empty

  /** batch that is being committed */
  private var batchInCommittment = CommittableOffsetBatch.empty

  /** last known committed batch result */
  private var batchCommitResult: Option[Try[CommittableOffsetBatch]] = None

  // ---- Consuming
  private def consume(offset: Committable): Unit = {
    offsetBatch = offsetBatch.updated(offset)
    if (offsetBatch.batchSize >= stage.committerSettings.maxBatch)
      commit(BatchSize, flush = false, pushDownStream = true)
    else if (isClosed(stage.in)) commit(UpstreamClosed, flush = false, pushDownStream = false)
    else tryPull(stage.in) // accumulating the batch
  }

  private def scheduleCommit(): Unit =
    scheduleOnce(CommitNow, stage.committerSettings.maxInterval)

  override protected def onTimer(timerKey: Any): Unit = timerKey match {
    case CommittingFlowStage.CommitNow => commit(Interval, flush = false, pushDownStream = true)
  }

  private def commit(triggeredBy: TriggerdBy, flush: Boolean, pushDownStream: Boolean): Unit = {
    if (offsetBatch.batchSize != 0) {
      log.debug("commit triggered by {} (flush={}, batchInCommittment.size={})",
                triggeredBy,
                flush,
                batchInCommittment.batchSize)
      batchInCommittment = offsetBatch
      // TODO: 'implement send and forget'
      offsetBatch
        .commitInternal(flush)
        .onComplete { t =>
          commitResultCB.invoke(pushDownStream -> t.map(_ => batchInCommittment))
        }(materializer.executionContext)
      offsetBatch = CommittableOffsetBatch.empty
    }
    scheduleCommit()
  }

  private val commitResultCB: AsyncCallback[(Boolean, Try[CommittableOffsetBatch])] =
    getAsyncCallback[(Boolean, Try[CommittableOffsetBatch])] {
      case (pushDownStream, result @ Success(batch)) =>
        batchInCommittment = CommittableOffsetBatch.empty
        batchCommitResult = Some(result)
        if (pushDownStream) { // TODO: check closed port and do emit?
          push(stage.out, batch)
        }
        checkForCompletion(result)
      case (_, msg @ Failure(exception)) =>
        batchInCommittment = CommittableOffsetBatch.empty
        batchCommitResult = Some(msg)
        decider(exception) match {
          case Supervision.Stop =>
            log.error("committing failed with {}", exception)
            closeAndFailStage(exception)
          case _ =>
            log.warning("ignored commit failure {}", exception)
        }
        checkForCompletion(msg)
    }

  // ---- handler and completion
  /** Keeps track of upstream completion signals until this stage shuts down. */
  private var upstreamCompletionState: Option[Try[Done]] = None

  setHandler(
    stage.in,
    new InHandler {
      def onPush(): Unit = {
        consume(grab(stage.in))
      }

      override def onUpstreamFinish(): Unit = {
        if (noActiveBatchesInProgress) {
          completeStage()
          batchCommitResult.foreach(r => streamCompletion.complete(r))
        } else {
          commit(UpstreamFinish, flush = false, pushDownStream = true)
          setKeepGoing(true)
          upstreamCompletionState = Some(Success(Done))
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        if (noActiveBatchesInProgress) {
          closeAndFailStage(ex)
        } else {
          commit(UpstreamFailure, flush = true, pushDownStream = true) // push batch in flight and then fail
          setKeepGoing(true)
          upstreamCompletionState = Some(Failure(ex))
        }
      }
    }
  )

  setHandler(
    stage.out,
    new OutHandler {
      def onPull(): Unit = if (!hasBeenPulled(stage.in)) {
        tryPull(stage.in)
      }
    }
  )

  private def noActiveBatchesInProgress: Boolean = {
    batchesEmpty(offsetBatch, batchInCommittment)
  }

  private def checkForCompletion(batch: Try[CommittableOffsetBatch]): Unit =
    if (isClosed(stage.in)) {
      if (noActiveBatchesInProgress) {
        upstreamCompletionState match {
          case Some(Success(_)) =>
            completeStage()
            batchCommitResult.foreach(r => streamCompletion.complete(r))
          case Some(Failure(ex)) =>
            closeAndFailStage(ex)
          case None =>
            closeAndFailStage(new IllegalStateException("Stage completed, but there is no info about status"))
        }
      }
    } else {
      log.debug(
        "checkForCompletion offsetBatch size={}, batchInCommittment size={}, batchCommitResult={}",
        offsetBatch.batchSize,
        batchInCommittment.batchSize,
        batchCommitResult
      )
    }

  override def postStop(): Unit = {
    log.debug("CommittingFlowStage stopped")
    super.postStop()
  }
}

private object CommittingFlowStage {
  val CommitNow = "flowStageCommit"

  private[akka] def batchEmpty(batch: CommittableOffsetBatch): Boolean = batch.batchSize == 0L

  private[akka] def batchesEmpty(batches: CommittableOffsetBatch*): Boolean = {
    batches.forall(batchEmpty)
  }
}
