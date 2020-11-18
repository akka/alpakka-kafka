/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import java.util.Objects
import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.{DoNotInherit, InternalApi}
import akka.kafka.internal.{CommittableOffsetBatchImpl, CommittedMarker}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future
import scala.runtime.AbstractFunction2

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
   * Carries offsets from Kafka for aggregation and committing by the [[scaladsl.Committer]]
   * or [[javadsl.Committer]].
   *
   * `Committable` may be a single offset in [[CommittableOffset]] or [[CommittableOffsetMetadata]],
   * or a number of offsets aggregated as [[CommittableOffsetBatch]].
   */
  @DoNotInherit trait Committable {
    @deprecated("use `Committer.flow` or `Committer.sink` instead of direct usage", "2.0.0")
    def commitScaladsl(): Future[Done]

    /**
     * @deprecated use `Committer.flow` or `Committer.sink` instead of direct usage, since 2.0.0
     */
    @java.lang.Deprecated
    @deprecated("use `Committer.flow` or `Committer.sink` instead of direct usage", "2.0.0")
    def commitJavadsl(): CompletionStage[Done]

    @InternalApi
    private[kafka] def commitInternal(): Future[Done]

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
  @DoNotInherit sealed trait CommittableOffset extends Committable {
    def partitionOffset: PartitionOffset
  }

  @DoNotInherit trait CommittableOffsetMetadata extends CommittableOffset {
    def metadata: String
  }

  /**
   * Offset position for a groupId, topic, partition.
   */
  sealed class PartitionOffset(val key: GroupTopicPartition, val offset: Long)
      extends Product2[GroupTopicPartition, Long]
      with Serializable {
    def withMetadata(metadata: String) =
      PartitionOffsetMetadata(key, offset, metadata)

    override def toString = s"PartitionOffset(key=$key,offset=$offset)"

    override def equals(other: Any): Boolean = other match {
      case that: PartitionOffset =>
        that.canEqual(this) && Objects.equals(this.key, that.key) &&
        Objects.equals(this.offset, that.offset)
      case _ => false
    }

    override def hashCode(): Int = Objects.hash(key, offset.asInstanceOf[java.lang.Long])

    // This code is for backwards compatibility when PartitionOffset was a case class.
    override def _1: GroupTopicPartition = key

    override def _2: Long = offset

    override def canEqual(that: Any): Boolean = that.isInstanceOf[PartitionOffset]

    def copy(key: GroupTopicPartition = this.key, offset: Long = this.offset): PartitionOffset =
      PartitionOffset(key, offset)
  }

  object PartitionOffset extends AbstractFunction2[GroupTopicPartition, Long, PartitionOffset] {
    def apply(key: GroupTopicPartition, offset: Long) = new PartitionOffset(key, offset)

    def unapply(arg: PartitionOffset): Option[(GroupTopicPartition, Long)] = Some((arg.key, arg.offset))
  }

  /**
   * Offset position and metadata for a groupId, topic, partition.
   */
  final case class PartitionOffsetMetadata(key: GroupTopicPartition, offset: Long, metadata: String)

  /**
   * Internal Api
   */
  @InternalApi private[kafka] final case class PartitionOffsetCommittedMarker(
      override val key: GroupTopicPartition,
      override val offset: Long,
      private[kafka] val committedMarker: CommittedMarker,
      private[kafka] val fromPartitionedSource: Boolean
  ) extends PartitionOffset(key, offset)

  /**
   * groupId, topic, partition key for an offset position.
   */
  final case class GroupTopicPartition(
      groupId: String,
      topic: String,
      partition: Int
  ) {
    def topicPartition: TopicPartition = new TopicPartition(topic, partition)
  }

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
  def createCommittableOffsetBatch[T <: Committable](offsets: java.util.List[T]): CommittableOffsetBatch = {
    import scala.jdk.CollectionConverters._
    CommittableOffsetBatch(offsets.asScala.toList)
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
    def offsets: Map[GroupTopicPartition, Long]

    /**
     * Java API: Get current offset positions
     */
    def getOffsets: java.util.Map[GroupTopicPartition, Long]

    /**
     * Internal API.
     *
     * Sends this offset batch to the consumer actor without expecting an answer.
     */
    @InternalApi
    private[kafka] def tellCommit(): CommittableOffsetBatch

    @InternalApi
    private[kafka] def tellCommitEmergency(): CommittableOffsetBatch

    @InternalApi
    private[kafka] def filter(p: GroupTopicPartition => Boolean): CommittableOffsetBatch

    /**
     * @return true if the batch contains no commits.
     */
    def isEmpty: Boolean
  }

}
