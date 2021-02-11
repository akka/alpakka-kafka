/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal
import akka.annotation.InternalApi
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

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
  def requestedOffsets: Map[TopicPartition, OffsetAndMetadata] = null
  def receivedMessages: Map[TopicPartition, SafeOffsetAndTimestamp] = null
  def committedOffsets: Map[TopicPartition, OffsetAndMetadata] = null

  def requested(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {}
  def received[K, V](records: ConsumerRecords[K, V]): Unit = {}
  def committed(offsets: java.util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
  def assignedPositionsAndSeek(assignedTps: Set[TopicPartition],
                               consumer: Consumer[_, _],
                               positionTimeout: java.time.Duration): Unit = {}
  def addProgressTrackingCallback(callback: ConsumerAssignmentTrackingListener): Unit = {}
}

@InternalApi
object ConsumerProgressTrackerNoop extends ConsumerProgressTracking {}

@InternalApi
final class ConsumerProgressTrackerImpl extends ConsumerProgressTracking {
  private var assignedOffsetsCallbacks: Seq[ConsumerAssignmentTrackingListener] = Seq()
  private var requestedOffsetsImpl = Map.empty[TopicPartition, OffsetAndMetadata]
  private var receivedMessagesImpl = Map.empty[TopicPartition, SafeOffsetAndTimestamp]
  private var committedOffsetsImpl = Map.empty[TopicPartition, OffsetAndMetadata]

  override def requestedOffsets: Map[TopicPartition, OffsetAndMetadata] = requestedOffsetsImpl

  override def receivedMessages: Map[TopicPartition, SafeOffsetAndTimestamp] = receivedMessagesImpl

  override def committedOffsets: Map[TopicPartition, OffsetAndMetadata] = committedOffsetsImpl

  override def addProgressTrackingCallback(callback: ConsumerAssignmentTrackingListener): Unit = {
    assignedOffsetsCallbacks = assignedOffsetsCallbacks :+ callback
  }

  override def received[K, V](received: ConsumerRecords[K, V]): Unit = {
    receivedMessagesImpl = receivedMessagesImpl ++ requestedOffsetsImpl.keys
        .map(tp => (tp, received.records(tp)))
        .filter { case (_, records) => records.size() > 0 }
        // get the last record, its the largest offset/most recent timestamp
        .map { case (partition, records) => (partition, records.get(records.size() - 1)) }
        .map {
          case (partition, record) =>
            partition -> new SafeOffsetAndTimestamp(record.offset(), record.timestamp())
        }
  }

  override def requested(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    requestedOffsetsImpl = requestedOffsets ++ offsets
  }

  override def committed(offsets: java.util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    committedOffsetsImpl = committedOffsets ++ offsets.asScala.toMap
  }

  override def revoke(revokedTps: Set[TopicPartition]): Unit = {
    requestedOffsetsImpl = requestedOffsetsImpl -- revokedTps
    committedOffsetsImpl = committedOffsets -- revokedTps
    receivedMessagesImpl = receivedMessages -- revokedTps
    assignedOffsetsCallbacks.foreach(_.revoke(revokedTps))
  }

  override def assignedPositions(assignedTps: Set[TopicPartition], assignedOffsets: Map[TopicPartition, Long]): Unit = {
    requestedOffsetsImpl = requestedOffsetsImpl ++ assignedOffsets.map {
        case (partition, offset) =>
          partition -> requestedOffsets.getOrElse(partition, new OffsetAndMetadata(offset))
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
