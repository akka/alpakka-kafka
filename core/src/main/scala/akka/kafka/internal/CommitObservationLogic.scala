/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.kafka.CommitWhen.{NextOffsetObserved, OffsetFirstObserved}
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.stream.stage.GraphStageLogic

/**
 * Shared commit observation logic between [[GraphStageLogic]] that facilitate offset commits,
 * such as [[CommitCollectorStage]] and [[CommittingProducerSinkStage]].  It's possible more
 * logic could be shared between these implementations.
 */
private[internal] trait CommitObservationLogic { self: GraphStageLogic =>
  def settings: CommitterSettings

  /** Batches offsets until a commit is triggered. */
  protected var offsetBatch: CommittableOffsetBatch = CommittableOffsetBatch.empty

  /** Deferred offset when `CommitterSetting.when == CommitWhen.NextOffsetObserved` **/
  protected var deferredOffset: Option[Committable] = None

  /**
   * Update the offset batch when applicable given `CommitWhen` settings.
   *
   * Returns true if the batch is ready to be committed.
   */
  def updateBatch(offset: Committable): Boolean = {
    val updateOffsetBatch = (settings.when, deferredOffset) match {
      case (OffsetFirstObserved, _) =>
        Some(offset)
      case (NextOffsetObserved, None) =>
        deferredOffset = Some(offset)
        None
      case (NextOffsetObserved, deferred @ Some(dOffset)) if dOffset != offset =>
        deferredOffset = Some(offset)
        deferred
    }

    for (offset <- updateOffsetBatch) offsetBatch = offsetBatch.updated(offset)

    offsetBatch.batchSize >= settings.maxBatch && updateOffsetBatch.isDefined
  }
}
