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

import scala.util.control.NonFatal

/**
 * INTERNAL API.
 *
 * Combined stage for committing incoming offsets in batches. Capable of emitting dynamic (reduced) size batch in case of
 * upstream failures. Support flushing on failure (for downstreams).
 */
@InternalApi
private[kafka] final class CommitCollectorStage[E, ACC](val committerSettings: CommitterSettings, val seed: ACC)(
    val aggregate: (ACC, E) => ACC
) extends GraphStage[FlowShape[(E, Committable), (ACC, CommittableOffsetBatch)]] {

  val in: Inlet[(E, Committable)] = Inlet[(E, Committable)]("FlowIn")
  val out: Outlet[(ACC, CommittableOffsetBatch)] = Outlet[(ACC, CommittableOffsetBatch)]("FlowOut")
  val shape: FlowShape[(E, Committable), (ACC, CommittableOffsetBatch)] = FlowShape(in, out)

  override def createLogic(
      inheritedAttributes: Attributes
  ): GraphStageLogic = {
    new CommitCollectorStageLogic(this, inheritedAttributes)
  }
}

private final class CommitCollectorStageLogic[E, ACC](
    stage: CommitCollectorStage[E, ACC],
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with CommitObservationLogic
    with StageIdLogging {

  import CommitCollectorStage._
  import CommitTrigger._

  final val settings: CommitterSettings = stage.committerSettings

  var accumulated: ACC = stage.seed
  final val aggregate = stage.aggregate

  override protected def logSource: Class[_] = classOf[CommitCollectorStageLogic[E, ACC]]

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
    log.debug("pushDownStream triggered by {}, outstanding batch {} and accumulated {}",
              triggeredBy,
              offsetBatch,
              accumulated)
    push(stage.out, (accumulated, offsetBatch))
    accumulated = stage.seed
    offsetBatch = CommittableOffsetBatch.empty
    scheduleCommit()
  }

  setHandler(
    stage.in,
    new InHandler {
      def onPush(): Unit = {
        val (elem, offset) = grab(stage.in)
        log.debug("Consuming offset {} and element {}", offset, elem)
        accumulated = try {
          aggregate(accumulated, elem)
        } catch {
          case NonFatal(e) =>
            // Commit buffered offsets as much as possible
            actionOnFailure()
            throw e
        }
        if (updateBatch(offset)) {
          // Push only of the outlet is available, a commit on interval might have taken the pending demand.
          // This is very hard to get tested consistently, so it gets this big comment instead.
          if (isAvailable(stage.out)) pushDownStream(BatchSize)
        } else tryPull(stage.in) // accumulating the batch
      }

      override def onUpstreamFinish(): Unit = {
        if (activeBatchInProgress) {
          log.debug("pushDownStream triggered by {}, outstanding batch {} and accumulated {}",
                    UpstreamFinish,
                    offsetBatch,
                    accumulated)
          emit(stage.out, (accumulated, offsetBatch))
        }
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        log.debug("onUpstreamFailure with exception {} with {} and {}", ex, offsetBatch, accumulated)
        actionOnFailure()
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

  private def actionOnFailure(): Unit = {
    if (activeBatchInProgress) {
      offsetBatch.tellCommitEmergency()
      accumulated = stage.seed
      offsetBatch = CommittableOffsetBatch.empty
    }
  }

}

private[akka] object CommitCollectorStage {
  val CommitNow = "flowStageCommit"
}
