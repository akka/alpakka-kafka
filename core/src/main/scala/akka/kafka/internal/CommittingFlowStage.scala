/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.kafka.internal.CommittingFlowStage.FlushableOffsetBatch
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream._
import akka.stream.stage._

import scala.util.{Failure, Try}

/**
 * INTERNAL API.
 *
 * Combined stage for producing, batching commits and committing.
 */
@InternalApi
private[kafka] final class CommittingFlowStage(val committerSettings: CommitterSettings)
    extends GraphStage[FlowShape[Committable, FlushableOffsetBatch]] {

  val in: Inlet[Committable] = Inlet[Committable]("FlowIn")
  val out: Outlet[FlushableOffsetBatch] = Outlet[FlushableOffsetBatch]("FlowOut")
  val shape: FlowShape[Committable, FlushableOffsetBatch] = FlowShape(in, out)

  override def createLogic(
      inheritedAttributes: Attributes
  ): GraphStageLogic = {
    new CommittingFlowStageLogic(this, inheritedAttributes)
  }
}

private final class CommittingFlowStageLogic(
    stage: CommittingFlowStage,
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with StageIdLogging {

  import CommitTrigger._
  import CommittingFlowStage._

  override protected def logSource: Class[_] = classOf[CommittingFlowStageLogic]

  // ---- initialization
  override def preStart(): Unit = {
    super.preStart()
    scheduleCommit()
    log.debug("CommittingFlowStage initialized")
  }

  inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

  protected val failsStageCallback: AsyncCallback[Throwable] = getAsyncCallback[Throwable](failStage)

  /** Batches offsets until a commit is triggered. */
  private var offsetBatch: CommittableOffsetBatch = CommittableOffsetBatch.empty

  // ---- Consuming
  private def consume(offset: Committable): Unit = {
    log.debug("Consuming offset {}", offset)
    offsetBatch = offsetBatch.updated(offset)
    if (offsetBatch.batchSize >= stage.committerSettings.maxBatch) pushDownStream(BatchSize, flush = false)(push)
    else tryPull(stage.in) // accumulating the batch
  }

  private def scheduleCommit(): Unit =
    scheduleOnce(CommitNow, stage.committerSettings.maxInterval)

  override protected def onTimer(timerKey: Any): Unit = timerKey match {
    case CommittingFlowStage.CommitNow => pushDownStream(Interval, flush = false)(push)
  }

  private def pushDownStream(triggeredBy: TriggerdBy, flush: Boolean)(
      emission: (Outlet[FlushableOffsetBatch], FlushableOffsetBatch) => Unit
  ): Unit = {
    if (activeBatchInProgress) {
      log.debug("pushDownStream triggered by {} (flush={}, offsetBatch.size={})",
                triggeredBy,
                flush,
                offsetBatch.batchSize)
      val outBatch = FlushableOffsetBatch(offsetBatch, flush)
      emission(stage.out, outBatch)
      offsetBatch = CommittableOffsetBatch.empty
    }
    scheduleCommit()
  }

  setHandler(
    stage.in,
    new InHandler {
      def onPush(): Unit = {
        consume(grab(stage.in))
      }

      override def onUpstreamFinish(): Unit = {
        if (noActiveBatchInProgress) {
          completeStage()
        } else {
          val flush = false
          pushDownStream(UpstreamFinish, flush)(emit[FlushableOffsetBatch])
          completeStage()
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        log.debug("Received onUpstreamFailure with exception {}", ex)
        if (noActiveBatchInProgress) {
          failStage(ex)
        } else {
          setKeepGoing(true)
          pushDownStream(UpstreamFailure, flush = true)(emit[FlushableOffsetBatch]) // push batch in flight and then fail
          failsStageCallback.invoke(ex)
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

  override def postStop(): Unit = {
    log.debug("CommittingFlowStage stopped")
    super.postStop()
  }

  private def noActiveBatchInProgress: Boolean = offsetBatch.batchSize == 0
  private def activeBatchInProgress: Boolean = !noActiveBatchInProgress
}

private[akka] object CommittingFlowStage {
  val CommitNow = "flowStageCommit"

  final case class FlushableOffsetBatch(batch: CommittableOffsetBatch, flush: Boolean)
}
