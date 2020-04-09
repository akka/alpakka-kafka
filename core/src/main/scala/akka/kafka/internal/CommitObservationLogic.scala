/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.kafka.CommitWhen.OffsetFirstObserved
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
  def updateBatch(committable: Committable): Boolean = {
    if (settings.when == OffsetFirstObserved) {
      offsetBatch = offsetBatch.updated(committable)
    } else { // CommitWhen.NextOffsetObserved
      val offset = committable.asInstanceOf[CommittableOffset]
      val gtp = offset.partitionOffset.key
      deferredOffsets.get(gtp) match {
        case Some(dOffset: CommittableOffset) if dOffset.partitionOffset.offset < offset.partitionOffset.offset =>
          deferredOffsets = deferredOffsets + (gtp -> offset)
          offsetBatch = offsetBatch.updated(dOffset)
        case None =>
          deferredOffsets = deferredOffsets + (gtp -> offset)
        case _ => ()
      }
    }
    offsetBatch.batchSize >= settings.maxBatch
  }

  /**
   * Clear any deferred offsets and return the count before emptied. This should only be called
   * once when a committing stage is shutting down.
   */
  def clearDeferredOffsets(): Int = {
    val size = deferredOffsets.size
    deferredOffsets = Map.empty
    size
  }
}
