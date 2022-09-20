/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal
import akka.annotation.InternalApi
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
 * Maintain our own OffsetAndTimestamp which can tolerate negative timestamps, which happen for old clients that
 * don't set timestamp explicitly.
 */
final case class SafeOffsetAndTimestamp(offset: Long, timestamp: Long)

/**
 * Listen for changes to the consumer assignments.
 */
@InternalApi
trait ConsumerAssignmentTrackingListener {
  def revoke(revokedTps: Set[TopicPartition]): Unit = {}
  def assignedPositions(assignedTps: Set[TopicPartition], assignedOffsets: Map[TopicPartition, Long]): Unit = {}
}

/**
 * Track the current state of the consumer: what offsets it has requested, received and committed, filtering by the
 * current assignments to the consumer. When a partition is assigned to the consumer for the first time, its
 * assigned offset is the current position of the partition (uses underlying Kafka Consumer to leverage the
 * configured offset-reset policy).
 */
@InternalApi
trait ConsumerProgressTracking extends ConsumerAssignmentTrackingListener {
  def commitRequested: Map[TopicPartition, OffsetAndMetadata] = null
  def receivedMessages: Map[TopicPartition, SafeOffsetAndTimestamp] = null
  def committedOffsets: Map[TopicPartition, OffsetAndMetadata] = null

  def commitRequested(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {}
  def received[K, V](records: ConsumerRecords[K, V]): Unit = {}
  def committed(offsets: java.util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
  def assignedPositionsAndSeek(assignedTps: Set[TopicPartition],
                               consumer: Consumer[_, _],
                               positionTimeout: java.time.Duration): Unit = {}
  def addProgressTrackingCallback(callback: ConsumerAssignmentTrackingListener): Unit = {}
}

@InternalApi
object ConsumerProgressTrackerNoop extends ConsumerProgressTracking {}

/**
 * Track the progress/state of the consumer. We generally try to be 'fast' with handling the partitions we track;
 * most of the smarts are expected to be handled outside this class. For example, we will update any offsets to
 * commit - [[commitRequested]] - without regard for what had previously been assigned
 * or revoked from the consumer. Thus, care should be taken when managing state of the consumer and making updates.
 *
 * The only case we try and be "smart" is during [[received]], where we will filter out offsets that are not
 * currently assigned; ensuring that we don't try to waste cycles on partitions that we no longer care about. This
 * matches downstream behavior where the [[SourceLogicBuffer]] filters out revoked partitions.
 */
@InternalApi
final class ConsumerProgressTrackerImpl extends ConsumerProgressTracking {
  private var assignedOffsetsCallbacks: Seq[ConsumerAssignmentTrackingListener] = Seq()
  private var commitRequestedOffsetsImpl = Map.empty[TopicPartition, OffsetAndMetadata]
  private var receivedMessagesImpl = Map.empty[TopicPartition, SafeOffsetAndTimestamp]
  private var committedOffsetsImpl = Map.empty[TopicPartition, OffsetAndMetadata]
  private var assignedPartitions = Set.empty[TopicPartition]

  override def commitRequested: Map[TopicPartition, OffsetAndMetadata] = commitRequestedOffsetsImpl

  override def receivedMessages: Map[TopicPartition, SafeOffsetAndTimestamp] = receivedMessagesImpl

  override def committedOffsets: Map[TopicPartition, OffsetAndMetadata] = committedOffsetsImpl

  override def addProgressTrackingCallback(callback: ConsumerAssignmentTrackingListener): Unit = {
    assignedOffsetsCallbacks = assignedOffsetsCallbacks :+ callback
  }

  override def received[K, V](received: ConsumerRecords[K, V]): Unit = {
    receivedMessagesImpl = receivedMessagesImpl ++ received
        .partitions()
        .asScala
        // only tracks the partitions that are currently assigned, as assignment is a synchronous interaction and polls
        // for an old consumer group epoch will not return (we get to make polls for the current generation). Supposing a
        // revoke completes and then the poll() is received for a previous epoch, we drop the records here (partitions
        // are no longer assigned to the consumer). If instead we get a poll() and then a revoke, we only track the
        // offsets for that short period of time and then they are revoked, so that is also safe.
        .intersect(assignedPartitions)
        .map(tp => (tp, received.records(tp)))
        // get the last record, its the largest offset/most recent timestamp
        .map { case (partition, records) => (partition, records.get(records.size() - 1)) }
        .map {
          case (partition, record) =>
            partition -> SafeOffsetAndTimestamp(record.offset(), record.timestamp())
        }
  }

  override def commitRequested(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    commitRequestedOffsetsImpl = commitRequested ++ offsets
  }

  override def committed(offsets: java.util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    committedOffsetsImpl = committedOffsets ++ offsets.asScala.toMap
  }

  override def revoke(revokedTps: Set[TopicPartition]): Unit = {
    commitRequestedOffsetsImpl = commitRequested -- revokedTps
    committedOffsetsImpl = committedOffsets -- revokedTps
    receivedMessagesImpl = receivedMessages -- revokedTps
    assignedPartitions = assignedPartitions -- revokedTps
    assignedOffsetsCallbacks.foreach(_.revoke(revokedTps))
  }

  override def assignedPositions(assignedTps: Set[TopicPartition], assignedOffsets: Map[TopicPartition, Long]): Unit = {
    assignedPartitions = assignedPartitions ++ assignedTps
    // though we haven't actually request/committed on assignment, they form the low-water mark for consumer
    // progress, so we update them when consumer is assigned. Consumer can always add more partitions - we only lose
    // them on revoke(), which is why this operation is only additive.
    commitRequestedOffsetsImpl = commitRequestedOffsetsImpl ++ assignedOffsets.map {
        case (partition, offset) =>
          partition -> commitRequested.getOrElse(partition, new OffsetAndMetadata(offset))
      }
    committedOffsetsImpl = committedOffsets ++ assignedOffsets.map {
        case (partition, offset) =>
          partition -> committedOffsets.getOrElse(partition, new OffsetAndMetadata(offset))
      }
    assignedOffsetsCallbacks.foreach(_.assignedPositions(assignedTps, assignedOffsets))
  }

  override def assignedPositionsAndSeek(assignedTps: Set[TopicPartition],
                                        consumer: Consumer[_, _],
                                        positionTimeout: java.time.Duration): Unit = {
    val assignedOffsets = assignedTps.map(tp => tp -> consumer.position(tp, positionTimeout)).toMap
    assignedPositions(assignedTps, assignedOffsets)
  }
}
