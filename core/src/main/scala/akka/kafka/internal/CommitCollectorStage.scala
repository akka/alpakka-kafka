/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.annotation.InternalApi
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.stream._
import akka.stream.stage._

/**
 * INTERNAL API.
 *
 * Combined stage for committing incoming offsets in batches. Capable of emitting dynamic (reduced) size batch in case of
 * upstream failures. Support flushing on failure (for downstreams).
 */
@InternalApi
private[kafka] final class CommitCollectorStage(val committerSettings: CommitterSettings)
    extends GraphStage[FlowShape[Committable, CommittableOffsetBatch]] {

  val in: Inlet[Committable] = Inlet[Committable]("FlowIn")
  val out: Outlet[CommittableOffsetBatch] = Outlet[CommittableOffsetBatch]("FlowOut")
  val shape: FlowShape[Committable, CommittableOffsetBatch] = FlowShape(in, out)

  override def createLogic(
      inheritedAttributes: Attributes
  ): GraphStageLogic = {
    new CommitCollectorStageLogic(this, inheritedAttributes)
  }
}

private final class CommitCollectorStageLogic(
    stage: CommitCollectorStage,
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with CommitObservationLogic
    with StageIdLogging {

  import CommitCollectorStage._
  import CommitTrigger._

  final val settings: CommitterSettings = stage.committerSettings

  override protected def logSource: Class[_] = classOf[CommitCollectorStageLogic]

  private var pushOnNextPull = false

  override def preStart(): Unit = {
    super.preStart()
    scheduleCommit()
    log.debug("CommitCollectorStage initialized")
  }

  private def scheduleCommit(): Unit =
    scheduleOnce(CommitNow, stage.committerSettings.maxInterval)

  override protected def onTimer(timerKey: Any): Unit = timerKey match {
    case CommitCollectorStage.CommitNow =>
      if (activeBatchInProgress) {
        // Push only of the outlet is available, as timers may occur outside of a push/pull cycle.
        // Otherwise instruct `onPull` to emit what is there when the next pull occurs.
        // This is very hard to get tested consistently, so it gets this big comment instead.
        if (isAvailable(stage.out)) pushDownStream(Interval)
        else pushOnNextPull = true
      } else scheduleCommit()
  }

  private def pushDownStream(triggeredBy: TriggerdBy): Unit = {
    log.debug("pushDownStream triggered by {}, outstanding batch {}", triggeredBy, offsetBatch)
    push(stage.out, offsetBatch)
    offsetBatch = CommittableOffsetBatch.empty
    scheduleCommit()
  }

  setHandler(
    stage.in,
    new InHandler {
      def onPush(): Unit = {
        val offset = grab(stage.in)
        log.debug("Consuming offset {}", offset)
        if (updateBatch(offset)) {
          // Push only of the outlet is available, a commit on interval might have taken the pending demand.
          // This is very hard to get tested consistently, so it gets this big comment instead.
          if (isAvailable(stage.out)) pushDownStream(BatchSize)
        } else tryPull(stage.in) // accumulating the batch
      }

      override def onUpstreamFinish(): Unit = {
        if (activeBatchInProgress) {
          log.debug("pushDownStream triggered by {}, outstanding batch {}", UpstreamFinish, offsetBatch)
          emit(stage.out, offsetBatch)
        }
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        log.debug("onUpstreamFailure with exception {} with {}", ex, offsetBatch)
        if (activeBatchInProgress) {
          offsetBatch.tellCommitEmergency()
          offsetBatch = CommittableOffsetBatch.empty
        }
        failStage(ex)
      }
    }
  )

  setHandler(
    stage.out,
    new OutHandler {
      def onPull(): Unit =
        if (pushOnNextPull) {
          pushDownStream(Interval)
          pushOnNextPull = false
        } else if (!hasBeenPulled(stage.in)) {
          tryPull(stage.in)
        }
    }
  )

  override def postStop(): Unit = {
    log.debug("CommitCollectorStage stopped")
    super.postStop()
  }

  private def activeBatchInProgress: Boolean = !offsetBatch.isEmpty
}

private[akka] object CommitCollectorStage {
  val CommitNow = "flowStageCommit"
}
