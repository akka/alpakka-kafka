/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{
  CommittableMessage,
  CommittableOffsetMetadata,
  GroupTopicPartition,
  TransactionalMessage,
  _
}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.OffsetFetchResponse

import scala.jdk.CollectionConverters._
import scala.collection.compat._
import scala.collection.immutable
import scala.compat.java8.FutureConverters.FutureOps
import scala.concurrent.Future

/** Internal API */
@InternalApi
private[kafka] trait MessageBuilder[K, V, Msg] {
  def createMessage(rec: ConsumerRecord[K, V]): Msg
}

/** Internal API */
@InternalApi
private[kafka] trait PlainMessageBuilder[K, V] extends MessageBuilder[K, V, ConsumerRecord[K, V]] {
  override def createMessage(rec: ConsumerRecord[K, V]): ConsumerRecord[K, V] = rec
}

/** Internal API */
@InternalApi
private[kafka] trait TransactionalMessageBuilderBase[K, V, Msg] extends MessageBuilder[K, V, Msg] {
  def groupId: String

  def committedMarker: CommittedMarker

  def onMessage(consumerMessage: ConsumerRecord[K, V]): Unit
}

/** Internal API */
@InternalApi
private[kafka] trait TransactionalMessageBuilder[K, V]
    extends TransactionalMessageBuilderBase[K, V, TransactionalMessage[K, V]] {
  override def createMessage(rec: ConsumerRecord[K, V]): TransactionalMessage[K, V] = {
    onMessage(rec)
    val offset = PartitionOffsetCommittedMarker(
      GroupTopicPartition(
        groupId = groupId,
        topic = rec.topic,
        partition = rec.partition
      ),
      offset = rec.offset,
      committedMarker
    )
    ConsumerMessage.TransactionalMessage(rec, offset)
  }
}

/** Internal API */
@InternalApi
private[kafka] trait TransactionalOffsetContextBuilder[K, V]
    extends TransactionalMessageBuilderBase[K, V, (ConsumerRecord[K, V], PartitionOffset)] {
  override def createMessage(rec: ConsumerRecord[K, V]): (ConsumerRecord[K, V], PartitionOffset) = {
    onMessage(rec)
    val offset = PartitionOffsetCommittedMarker(
      GroupTopicPartition(
        groupId = groupId,
        topic = rec.topic,
        partition = rec.partition
      ),
      offset = rec.offset,
      committedMarker
    )
    (rec, offset)
  }
}

/** Internal API */
@InternalApi
private[kafka] trait CommittableMessageBuilder[K, V] extends MessageBuilder[K, V, CommittableMessage[K, V]] {
  def groupId: String
  def committer: KafkaAsyncConsumerCommitterRef
  def metadataFromRecord(record: ConsumerRecord[K, V]): String

  override def createMessage(rec: ConsumerRecord[K, V]): CommittableMessage[K, V] = {
    val offset = ConsumerMessage.PartitionOffset(
      GroupTopicPartition(
        groupId = groupId,
        topic = rec.topic,
        partition = rec.partition
      ),
      offset = rec.offset
    )
    ConsumerMessage.CommittableMessage(rec, CommittableOffsetImpl(offset, metadataFromRecord(rec))(committer))
  }
}

private[kafka] object CommittableMessageBuilder {
  val NoMetadataFromRecord: ConsumerRecord[_, _] => String = (_: ConsumerRecord[_, _]) =>
    OffsetFetchResponse.NO_METADATA
}

/** Internal API */
@InternalApi
private[kafka] trait OffsetContextBuilder[K, V]
    extends MessageBuilder[K, V, (ConsumerRecord[K, V], CommittableOffset)] {
  def groupId: String
  def committer: KafkaAsyncConsumerCommitterRef
  def metadataFromRecord(record: ConsumerRecord[K, V]): String

  override def createMessage(rec: ConsumerRecord[K, V]): (ConsumerRecord[K, V], CommittableOffset) = {
    val offset = ConsumerMessage.PartitionOffset(
      GroupTopicPartition(
        groupId = groupId,
        topic = rec.topic,
        partition = rec.partition
      ),
      offset = rec.offset
    )
    (rec, CommittableOffsetImpl(offset, metadataFromRecord(rec))(committer))
  }
}

/** Internal API */
@InternalApi private[kafka] final case class CommittableOffsetImpl(
    override val partitionOffset: ConsumerMessage.PartitionOffset,
    override val metadata: String
)(
    val committer: KafkaAsyncConsumerCommitterRef
) extends CommittableOffsetMetadata {
  override def commitScaladsl(): Future[Done] = committer.commitSingle(this)
  override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava
  override val batchSize: Long = 1
}

/** Internal API */
@InternalApi
private[kafka] trait CommittedMarker {

  /** Marks offsets as already committed */
  def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Done]

  /** Marks committing failure */
  def failed(): Unit
}

/** Internal API */
@InternalApi
private[kafka] final class CommittableOffsetBatchImpl(
    val offsetsAndMetadata: Map[GroupTopicPartition, OffsetAndMetadata],
    val committers: Map[String, KafkaAsyncConsumerCommitterRef],
    override val batchSize: Long
) extends CommittableOffsetBatch {
  def offsets: Map[GroupTopicPartition, Long] = offsetsAndMetadata.view.mapValues(_.offset() - 1L).toMap

  def updated(committable: Committable): CommittableOffsetBatch = committable match {
    case offset: CommittableOffset => updatedWithOffset(offset)
    case batch: CommittableOffsetBatch => updatedWithBatch(batch)
  }

  private[internal] def committerFor(groupId: String) = committers.getOrElse(
    groupId,
    throw new IllegalStateException(s"Unknown committer, got [$groupId]")
  )

  private[internal] def groupIdOffsetMaps: Map[String, Map[TopicPartition, OffsetAndMetadata]] =
    offsetsAndMetadata.groupBy(_._1.groupId).map {
      case (groupId, offsetsMap) =>
        val offsets: Map[TopicPartition, OffsetAndMetadata] = offsetsMap.map {
          case (gtp, offset) => gtp.topicPartition -> offset
        }
        (groupId, offsets)
    }

  private def updatedWithOffset(committableOffset: CommittableOffset): CommittableOffsetBatch = {
    val partitionOffset = committableOffset.partitionOffset
    val key = partitionOffset.key
    val metadata = committableOffset match {
      case offset: CommittableOffsetMetadata =>
        offset.metadata
      case _ =>
        OffsetFetchResponse.NO_METADATA
    }

    val newOffsets =
      offsetsAndMetadata.updated(key, new OffsetAndMetadata(committableOffset.partitionOffset.offset + 1L, metadata))

    val committer = committableOffset match {
      case c: CommittableOffsetImpl => c.committer
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffset, got [${committableOffset.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
    }

    val newCommitters = committers.get(key.groupId) match {
      case Some(s) =>
        require(
          s == committer,
          s"CommittableOffset [$committableOffset] committer for groupId [${key.groupId}] " +
          s"must be same as the other with this groupId. Expected [$s], got [$committer]"
        )
        committers
      case None =>
        committers.updated(key.groupId, committer)
    }

    new CommittableOffsetBatchImpl(newOffsets, newCommitters, batchSize + 1)
  }

  private def updatedWithBatch(committableOffsetBatch: CommittableOffsetBatch): CommittableOffsetBatch =
    committableOffsetBatch match {
      case c: CommittableOffsetBatchImpl =>
        val newOffsetsAndMetadata = offsetsAndMetadata ++ c.offsetsAndMetadata
        val newCommitters = c.committers.foldLeft(committers) {
          case (acc, (groupId, committer)) =>
            acc.get(groupId) match {
              case Some(s) =>
                require(
                  s == committer,
                  s"CommittableOffsetBatch [$committableOffsetBatch] committer for groupId [$groupId] " +
                  s"must be same as the other with this groupId. Expected [$s], got [$committer]"
                )
                acc
              case None =>
                acc.updated(groupId, committer)
            }
        }
        new CommittableOffsetBatchImpl(newOffsetsAndMetadata,
                                       newCommitters,
                                       batchSize + committableOffsetBatch.batchSize)
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffsetBatch, got [${committableOffsetBatch.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
        )
    }

  override def getOffsets(): java.util.Map[GroupTopicPartition, Long] =
    offsets.asJava

  override def toString(): String =
    s"CommittableOffsetBatch(batchSize=$batchSize, ${offsets.mkString(", ")})"

  override def commitScaladsl(): Future[Done] =
    if (batchSize == 0L)
      Future.successful(Done)
    else {
      committers.head._2.commit(this)
    }

  override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava

}
