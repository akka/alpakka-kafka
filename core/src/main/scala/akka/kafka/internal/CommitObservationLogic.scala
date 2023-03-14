/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
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
      committable match {
        case single: CommittableOffset =>
          val gtp = single.partitionOffset.key
          updateBatchForPartition(gtp, single, single.partitionOffset.offset)
        case batch: CommittableOffsetBatchImpl =>
          for { (gtp, offsetAndMetadata) <- batch.offsetsAndMetadata } updateBatchForPartition(
            gtp,
            batch.filter(_.equals(gtp)),
            offsetAndMetadata.offset()
          )
        case null =>
          throw new IllegalArgumentException(
            s"Unknown Committable implementation, got [null]"
          )
        case unknownImpl =>
          throw new IllegalArgumentException(
            s"Unknown Committable implementation, got [${unknownImpl.getClass.getName}]"
          )

      }
    }
    offsetBatch.batchSize >= settings.maxBatch
  }

  private def updateBatchForPartition(gtp: GroupTopicPartition, committable: Committable, offset: Long): Unit =
    deferredOffsets.get(gtp) match {
      case Some(dOffset: CommittableOffset) if dOffset.partitionOffset.offset < offset =>
        deferredOffsets = deferredOffsets + (gtp -> committable)
        offsetBatch = offsetBatch.updated(dOffset)
      case Some(dOffsetBatch: CommittableOffsetBatch)
          if dOffsetBatch.offsets.contains(gtp) && dOffsetBatch.offsets
            .get(gtp)
            .head < offset =>
        deferredOffsets = deferredOffsets + (gtp -> committable)
        offsetBatch = offsetBatch.updated(dOffsetBatch)
      case None =>
        deferredOffsets = deferredOffsets + (gtp -> committable)
      case _ => ()
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
