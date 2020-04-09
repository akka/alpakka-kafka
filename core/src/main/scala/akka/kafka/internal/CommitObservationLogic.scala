/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.kafka.CommitWhen.{NextOffsetObserved, OffsetFirstObserved}
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffset, CommittableOffsetBatch, GroupTopicPartition}
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

  /** Deferred offsets when `CommitterSetting.when == CommitWhen.NextOffsetObserved` **/
  private var deferredOffsets: Map[GroupTopicPartition, Committable] = Map.empty

  /**
   * Update the offset batch when applicable given `CommitWhen` settings. Returns true if the
   * batch is ready to be committed.
   */
  def updateBatch(offset: Committable): Boolean = {
    val gtp = offset.asInstanceOf[CommittableOffset].partitionOffset.key
    val updateOffsetBatch = (settings.when, deferredOffsets.get(gtp)) match {
      case (OffsetFirstObserved, _) =>
        Some(offset)
      case (NextOffsetObserved, None) =>
        deferredOffsets = deferredOffsets + (gtp -> offset)
        None
      case (NextOffsetObserved, deferred @ Some(dOffset)) if dOffset != offset =>
        deferredOffsets = deferredOffsets + (gtp -> offset)
        deferred
      case _ => None
    }
    for (offset <- updateOffsetBatch) offsetBatch = offsetBatch.updated(offset)
    offsetBatch.batchSize >= settings.maxBatch
  }

  /**
   * Clear any deferred offsets and return the count before emptied. This should only be called
   * once when a committing stages is shutting down.
   */
  def clearDeferredOffsets(): Int = {
    val size = deferredOffsets.size
    deferredOffsets = Map.empty
    size
  }
}
