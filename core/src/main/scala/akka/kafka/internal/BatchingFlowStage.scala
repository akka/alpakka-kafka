/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.annotation.InternalApi
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.kafka.internal.BatchingFlowStage.FlushableOffsetBatch
import akka.stream._
import akka.stream.stage._

import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API.
 *
 * Combined stage for committing incoming offsets in batches. Capable of emitting dynamic (reduced) size batch in case of
 * upstream failures. Support flushing on failure (for downstreams).
 */
@InternalApi
private[kafka] final class BatchingFlowStage(val committerSettings: CommitterSettings)
    extends GraphStage[FlowShape[Committable, Try[FlushableOffsetBatch]]] {

  val in: Inlet[Committable] = Inlet[Committable]("FlowIn")
  val out: Outlet[Try[FlushableOffsetBatch]] = Outlet[Try[FlushableOffsetBatch]]("FlowOut")
  val shape: FlowShape[Committable, Try[FlushableOffsetBatch]] = FlowShape(in, out)

  override def createLogic(
      inheritedAttributes: Attributes
  ): GraphStageLogic = {
    new BatchingFlowStageLogic(this, inheritedAttributes)
  }
}

private final class BatchingFlowStageLogic(
    stage: BatchingFlowStage,
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with StageIdLogging {

  import BatchingFlowStage._
  import CommitTrigger._

  override protected def logSource: Class[_] = classOf[BatchingFlowStageLogic]

  // ---- initialization
  override def preStart(): Unit = {
    super.preStart()
    scheduleCommit()
    log.debug("BatchingFlowStage initialized")
  }

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
    case BatchingFlowStage.CommitNow => pushDownStream(Interval, flush = false)(push)
  }

  private def pushDownStream(triggeredBy: TriggerdBy, flush: Boolean)(
      emission: (Outlet[Try[FlushableOffsetBatch]], Try[FlushableOffsetBatch]) => Unit
  ): Unit = {
    if (activeBatchInProgress) {
      log.debug("pushDownStream triggered by {} (flush={}, offsetBatch.size={})",
                triggeredBy,
                flush,
                offsetBatch.batchSize)
      val outBatch = Success(FlushableOffsetBatch(offsetBatch, flush))
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
          pushDownStream(UpstreamFinish, flush)(emit[Try[FlushableOffsetBatch]])
          completeStage()
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        def emissionWithException(outlet: Outlet[Try[FlushableOffsetBatch]], elem: Try[FlushableOffsetBatch]): Unit = {
          emitMultiple(outlet, List(elem, Failure(ex)))
        }

        log.debug("Received onUpstreamFailure with exception {}", ex)
        if (noActiveBatchInProgress) {
          failStage(ex)
        } else {
          setKeepGoing(true)
          pushDownStream(UpstreamFailure, flush = true)(emissionWithException) // push batch in flight and then fail
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
    log.debug("BatchingFlowStage stopped")
    super.postStop()
  }

  private def noActiveBatchInProgress: Boolean = offsetBatch.batchSize == 0
  private def activeBatchInProgress: Boolean = !noActiveBatchInProgress
}

private[akka] object BatchingFlowStage {
  val CommitNow = "flowStageCommit"

  final case class FlushableOffsetBatch(batch: CommittableOffsetBatch, flush: Boolean)
}
