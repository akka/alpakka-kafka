/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.kafka.OffsetResetProtectionSettings
import akka.kafka.internal.KafkaConsumerActor.Internal.Seek
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

@InternalApi
trait ConsumerResetProtection {

  /**
   * Check the offsets of the records for each partition are not "much older" than the records that we have seen thus
   * far for the partition. Records/partitions that appear to have rewound to a much earlier time (as defined by the
   * configured threshold) are dropped and the consumer is seeked back to the last safe offset for that partition - the
   * last committed offset for the partition. Records that are newer - or within the rewind threshold - are passed
   * through.
   */
  def protect[K, V](consumer: ActorRef, records: ConsumerRecords[K, V]): ConsumerRecords[K, V]
}

@InternalApi
object ConsumerResetProtection {
  def apply[K, V](log: LoggingAdapter,
                  setttings: OffsetResetProtectionSettings,
                  progress: () => ConsumerProgressTracking): ConsumerResetProtection = {
    if (setttings.enable) new Impl(log, setttings, progress()) else ConsumerResetProtection.Noop
  }

  private object Noop extends ConsumerResetProtection {
    override def protect[K, V](consumer: ActorRef, records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = records
  }

  private final class Impl(log: LoggingAdapter,
                           resetProtection: OffsetResetProtectionSettings,
                           progress: ConsumerProgressTracking)
      extends ConsumerResetProtection {
    override def protect[K, V](consumer: ActorRef, records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
      val safe: java.util.Map[TopicPartition, java.util.List[ConsumerRecord[K, V]]] =
        records
          .partitions()
          .asScala
          .flatMap(maybeProtectRecords(consumer, _, records).toList)
          .toMap
          .asJava

      new ConsumerRecords[K, V](safe)
    }

    /**
     * Check to see if the records can be safely passed to the consumer (no reset found). If not, then tells the
     * consumer to seek back to the last safe location (last committed offset) and try pulling again. Note that if
     * the consumer commits less frequently than the configured protection interval, then they will be constantly
     * seeking back to the last committed offset.
     * @return [[Some]] records if they can be safely passed to the consumer, returns [[None]] otherwise.
     */
    private def maybeProtectRecords[K, V](
        consumer: ActorRef,
        tp: TopicPartition,
        records: ConsumerRecords[K, V]
    ): Option[(TopicPartition, util.List[ConsumerRecord[K, V]])] = {
      val partitionRecords: util.List[ConsumerRecord[K, V]] = records.records(tp)
      progress.commitRequested.get(tp) match {
        case Some(requested) => protectPartition(consumer, tp, requested, partitionRecords)
        case None =>
          // it's a partition that we have no information on, so assume it's safe and continue because it's likely
          // due to a rebalance, in which we have already reset to the committed offset, which is safe
          Some((tp, partitionRecords))
      }
    }

    /**
     * We found that we have previously committed the [[OffsetAndMetadata]] for the [[TopicPartition]], check it
     * against the partition records to ensure that we haven't exceeded the threshold.  If the records are within the
     * threshold, we just return the given records. If there are records found outside the threshold:
     *
     * 1. Drop the records returned for the partition (they are outside the threshold) by returning `None`.
     * 2. Request a seek to the latest committed offset of the partition (known to be "safe").
     * 3. New records for the partition arrive with the next poll, which should be within the threshold.
     */
    private def protectPartition[K, V](
        consumer: ActorRef,
        tp: TopicPartition,
        previouslyCommitted: OffsetAndMetadata,
        partitionRecords: util.List[ConsumerRecord[K, V]]
    ): Option[(TopicPartition, util.List[ConsumerRecord[K, V]])] = {
      val threshold = new RecordThreshold(previouslyCommitted.offset(), progress.receivedMessages.get(tp))
      if (threshold.recordsExceedThreshold(threshold, partitionRecords)) {
        // requested and committed are assumed to be kept in-sync, so this _should_ be safe. Fails
        // catastrophically if this is not the case
        val committed = progress.committedOffsets(tp)
        val requestVersusCommitted = previouslyCommitted.offset() - committed.offset()
        if (resetProtection.offsetThreshold < Long.MaxValue &&
            requestVersusCommitted > resetProtection.offsetThreshold) {
          log.warning(
            s"Your last commit request $previouslyCommitted is more than the configured threshold from the last" +
            s"committed offset ($committed) for $tp. See " +
            "https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html#setting-offset-threshold-appropriately for more info."
          )
        }
        log.warning(
          s"Dropping offsets for partition $tp - received an offset which is less than allowed $threshold " +
          s"from the  last requested offset (threshold: $threshold). Seeking to the latest known safe (committed " +
          s"or assigned) offset: $committed. See  " +
          "https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html#unexpected-consumer-offset-reset" +
          "for more information."
        )
        consumer ! Seek(Map(tp -> committed.offset()))
        None
      } else {
        Some((tp, partitionRecords))
      }
    }

    /**
     * Determine if a record is within threshold for the partition.
     * @param previouslyRequestedOffset the offset that the consumer last requested
     * @param maybeReceived the [[SafeOffsetAndTimestamp]] we last received. If its the first time we receive a record
     *                      for this partition, we cannot say if there has been a reset based on the timestamp, this
     *                      should be [[None]], in which case we skip the timestamp based check.
     */
    private class RecordThreshold(previouslyRequestedOffset: Long, maybeReceived: Option[SafeOffsetAndTimestamp]) {
      private val offsetThreshold = previouslyRequestedOffset - resetProtection.offsetThreshold
      // we can skip checking if:
      // (A) the time threshold is very large (i.e. unset), or
      // (B) we haven't received a timestamp for the that partition, in which case also cannot make a claim to check.
      private val timeThreshold: Option[Long] = maybeReceived
        .flatMap { safeOffset =>
          val threshold = safeOffset.timestamp - resetProtection.timeThreshold.toMillis
          if (threshold > 0) Some(threshold) else None
        }

      /**
       * Check if the records' offsets exceed the allowed threshold.
       * @return `true` if the records in the batch have gone outside the threshold, `false` otherwise.
       */
      def recordsExceedThreshold[K, V](threshold: RecordThreshold,
                                       partitionRecords: util.List[ConsumerRecord[K, V]]): Boolean = {
        var exceedThreshold = false
        // rather than check all the records in the batch, trust that Kafka has given them to us in order, and just
        // check the first and last offsets in the batch.
        if (partitionRecords.size() > 0) {
          exceedThreshold = threshold.checkExceedsThreshold(partitionRecords.get(0))
          if (!exceedThreshold && partitionRecords.size() > 1) {
            exceedThreshold = threshold.checkExceedsThreshold(partitionRecords.get(partitionRecords.size() - 1))
          }
        }
        exceedThreshold
      }

      def checkExceedsThreshold[K, V](record: ConsumerRecord[K, V]): Boolean = {
        record.offset() < offsetThreshold ||
        // timestamp can be set to -1 for some older client versions, ensure we don't penalize that
        timeThreshold.exists(
          threshold =>
            record.timestamp() != ConsumerRecord.NO_TIMESTAMP &&
            record.timestamp() < threshold
        )
      }

      override def toString: String = s"max-offset: $offsetThreshold, max-timestamp: $timeThreshold"
    }
  }
}
