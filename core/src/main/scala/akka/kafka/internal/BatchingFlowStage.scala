/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.annotation.InternalApi
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
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
    extends GraphStage[FlowShape[Committable, Try[CommittableOffsetBatch]]] {

  val in: Inlet[Committable] = Inlet[Committable]("FlowIn")
  val out: Outlet[Try[CommittableOffsetBatch]] = Outlet[Try[CommittableOffsetBatch]]("FlowOut")
  val shape: FlowShape[Committable, Try[CommittableOffsetBatch]] = FlowShape(in, out)

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
    if (offsetBatch.batchSize >= stage.committerSettings.maxBatch) pushDownStream(BatchSize)(push)
    else tryPull(stage.in) // accumulating the batch
  }

  private def scheduleCommit(): Unit =
    scheduleOnce(CommitNow, stage.committerSettings.maxInterval)

  override protected def onTimer(timerKey: Any): Unit = timerKey match {
    case BatchingFlowStage.CommitNow => pushDownStream(Interval)(push)
  }

  private def pushDownStream(triggeredBy: TriggerdBy)(
      emission: (Outlet[Try[CommittableOffsetBatch]], Try[CommittableOffsetBatch]) => Unit
  ): Unit = {
    if (activeBatchInProgress) {
      log.debug("pushDownStream triggered by {}, outstanding batch {}", triggeredBy, offsetBatch)
      val outBatch = Success(offsetBatch)
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
          pushDownStream(UpstreamFinish)(emit[Try[CommittableOffsetBatch]])
          completeStage()
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        log.debug("Received onUpstreamFailure with exception {}", ex)
        if (noActiveBatchInProgress) {
          failStage(ex)
        } else {
          commitAndPushWithFailure(ex)
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

  private def commitAndPushWithFailure(ex: Throwable): Unit = {
    setKeepGoing(true)
    if (offsetBatch.batchSize != 0) {
      log.debug("committing batch in flight on failure {}", offsetBatch)
      val batchInFlight = offsetBatch
      offsetBatch
        .commitInternal(flush = true)
        .onComplete { t =>
          commitResultOnFailureCallback.invoke(ex)
        }(materializer.executionContext)
    }
  }

  def emissionWithException(ex: Throwable)(outlet: Outlet[Try[CommittableOffsetBatch]],
                                           elem: Try[CommittableOffsetBatch]): Unit = {
    emitMultiple(outlet, List(elem, Failure(ex)))
  }

  private val commitResultOnFailureCallback: AsyncCallback[Throwable] = {
    getAsyncCallback[Throwable] { ex =>
      pushDownStream(UpstreamFailure)(emissionWithException(ex))
      offsetBatch = CommittableOffsetBatch.empty
    }
  }

  override def postStop(): Unit = {
    log.debug("BatchingFlowStage stopped")
    super.postStop()
  }

  private def noActiveBatchInProgress: Boolean = offsetBatch.batchSize == 0
  private def activeBatchInProgress: Boolean = !noActiveBatchInProgress
}

private[akka] object BatchingFlowStage {
  val CommitNow = "flowStageCommit"
}
