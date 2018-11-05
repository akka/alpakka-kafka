/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import java.util.{Map => JMap}
import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.DoNotInherit
import akka.kafka.internal.CommittableOffsetBatchImpl
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

/**
 * Classes that are used in both [[javadsl.Consumer]] and
 * [[scaladsl.Consumer]].
 */
object ConsumerMessage {

  /**
   * Output element of `committableSource`.
   * The offset can be committed via the included [[CommittableOffset]].
   */
  final case class CommittableMessage[K, V](
      record: ConsumerRecord[K, V],
      committableOffset: CommittableOffset
  )

  /**
   * Output element of `transactionalSource`.
   * The offset is automatically committed as by the Producer
   */
  final case class TransactionalMessage[K, V](
      record: ConsumerRecord[K, V],
      partitionOffset: PartitionOffset
  )

  /**
   * Commit an offset that is included in a [[CommittableMessage]].
   * If you need to store offsets in anything other than Kafka, this API
   * should not be used.
   *
   * This interface might move into `akka.stream`
   */
  @DoNotInherit trait Committable {
    def commitScaladsl(): Future[Done]
    def commitJavadsl(): CompletionStage[Done]

    /**
     * Get a number of processed messages this committable contains
     */
    def batchSize: Long
  }

  /**
   * Included in [[CommittableMessage]]. Makes it possible to
   * commit an offset with the [[Committable#commitScaladsl]] / [[Committable#commitJavadsl]] method
   * or aggregate several offsets in a [[CommittableOffsetBatch batch]]
   * before committing.
   *
   * Note that the offset position that is committed to Kafka will automatically
   * be one more than the `offset` of the message, because the committed offset
   * should be the next message your application will consume,
   * i.e. lastProcessedMessageOffset + 1.
   */
  @DoNotInherit trait CommittableOffset extends Committable {
    def partitionOffset: PartitionOffset
  }

  @DoNotInherit trait CommittableOffsetMetadata extends CommittableOffset {
    def metadata: String
  }

  /**
   * Offset position for a groupId, topic, partition.
   */
  final case class PartitionOffset(key: GroupTopicPartition, offset: Long) {
    def withMetadata(metadata: String) =
      PartitionOffsetMetadata(key, offset, metadata)
  }

  /**
   * Offset position and metadata for a groupId, topic, partition.
   */
  final case class PartitionOffsetMetadata(key: GroupTopicPartition, offset: Long, metadata: String)

  /**
   * groupId, topic, partition key for an offset position.
   */
  final case class GroupTopicPartition(
      groupId: String,
      topic: String,
      partition: Int
  )

  object CommittableOffsetBatch {
    val empty: CommittableOffsetBatch = new CommittableOffsetBatchImpl(Map.empty, Map.empty, 0)

    /**
     * Scala API:
     * Create an offset batch out of a first offsets.
     */
    def apply(offset: CommittableOffset): CommittableOffsetBatch = empty.updated(offset)

    /**
     * Scala API:
     * Create an offset batch out of a list of offsets.
     */
    def apply(offsets: Seq[Committable]): CommittableOffsetBatch =
      offsets.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) =>
        batch.updated(elem)
      }
  }

  val emptyCommittableOffsetBatch: CommittableOffsetBatch = CommittableOffsetBatch.empty

  /**
   * Java API:
   * Create an offset batch out of a first offsets.
   */
  def createCommittableOffsetBatch(first: CommittableOffset): CommittableOffsetBatch = CommittableOffsetBatch(first)

  /**
   * Java API:
   * Create an offset batch out of a list of offsets.
   */
  def createCommittableOffsetBatch(offsets: java.util.List[CommittableOffset]): CommittableOffsetBatch = {
    import scala.collection.JavaConverters._
    CommittableOffsetBatch(offsets.asScala)
  }

  /**
   * For improved efficiency it is good to aggregate several [[CommittableOffset]],
   * using this class, before [[Committable#commitScaladsl committing]] them. Start with
   * the [[CommittableOffsetBatch#empty empty]] batch.
   */
  @DoNotInherit trait CommittableOffsetBatch extends Committable {

    /**
     * Add/overwrite an offset position from another committable.
     */
    def updated(offset: Committable): CommittableOffsetBatch

    /**
     * Scala API: Get current offset positions
     */
    def offsets(): Map[GroupTopicPartition, Long]

    /**
     * Java API: Get current offset positions
     */
    def getOffsets(): JMap[GroupTopicPartition, Long]

  }

}
