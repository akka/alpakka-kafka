/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.concurrent.CompletionStage
import java.util.{Map => JMap}

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
import org.apache.kafka.common.requests.OffsetFetchResponse

import scala.collection.JavaConverters._
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
  override def createMessage(rec: ConsumerRecord[K, V]) = rec
}

/** Internal API */
@InternalApi
private[kafka] trait TransactionalMessageBuilder[K, V] extends MessageBuilder[K, V, TransactionalMessage[K, V]] {
  def groupId: String

  override def createMessage(rec: ConsumerRecord[K, V]) = {
    val offset = ConsumerMessage.PartitionOffset(
      GroupTopicPartition(
        groupId = groupId,
        topic = rec.topic,
        partition = rec.partition
      ),
      offset = rec.offset
    )
    ConsumerMessage.TransactionalMessage(rec, offset)
  }
}

/** Internal API */
@InternalApi
private[kafka] trait CommittableMessageBuilder[K, V] extends MessageBuilder[K, V, CommittableMessage[K, V]] {
  def groupId: String
  def committer: Committer
  def metadataFromRecord(record: ConsumerRecord[K, V]): String

  override def createMessage(rec: ConsumerRecord[K, V]) = {
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

private final case class CommittableOffsetImpl(override val partitionOffset: ConsumerMessage.PartitionOffset,
                                               override val metadata: String)(
    val committer: Committer
) extends CommittableOffsetMetadata {
  override def commitScaladsl(): Future[Done] =
    committer.commit(immutable.Seq(partitionOffset.withMetadata(metadata)))
  override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava
}

/** Internal API */
@InternalApi
private[kafka] trait Committer {
  // Commit all offsets (of different topics) belonging to the same stage
  def commit(offsets: immutable.Seq[PartitionOffsetMetadata]): Future[Done]
  def commit(batch: CommittableOffsetBatch): Future[Done]
}

/** Internal API */
@InternalApi
private[kafka] final class CommittableOffsetBatchImpl(
    val offsetsAndMetadata: Map[GroupTopicPartition, OffsetAndMetadata],
    val stages: Map[String, Committer]
) extends CommittableOffsetBatch {
  def offsets = offsetsAndMetadata.mapValues(_.offset())

  def updated(committableOffset: CommittableOffset): CommittableOffsetBatch = {
    val partitionOffset = committableOffset.partitionOffset
    val key = partitionOffset.key
    val metadata = committableOffset match {
      case offset: CommittableOffsetMetadata =>
        offset.metadata
      case _ =>
        OffsetFetchResponse.NO_METADATA
    }

    val newOffsets =
      offsetsAndMetadata.updated(key, new OffsetAndMetadata(committableOffset.partitionOffset.offset, metadata))

    val stage = committableOffset match {
      case c: CommittableOffsetImpl => c.committer
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffset, got [${committableOffset.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
    }

    val newStages = stages.get(key.groupId) match {
      case Some(s) =>
        require(s == stage,
                s"CommittableOffset [$committableOffset] origin stage must be same as other " +
                s"stage with same groupId. Expected [$s], got [$stage]")
        stages
      case None =>
        stages.updated(key.groupId, stage)
    }

    new CommittableOffsetBatchImpl(newOffsets, newStages)
  }

  def updated(committableOffsetBatch: CommittableOffsetBatch): CommittableOffsetBatch =
    committableOffsetBatch match {
      case c: CommittableOffsetBatchImpl =>
        val newOffsetsAndMetadata = offsetsAndMetadata ++ c.offsetsAndMetadata
        val newStages = c.stages.foldLeft(stages) {
          case (acc, (groupId, stage)) =>
            acc.get(groupId) match {
              case Some(s) =>
                require(
                  s == stage,
                  s"CommittableOffsetBatch [$committableOffsetBatch] origin stage for groupId [$groupId] " +
                  s"must be same as other stage with same groupId. Expected [$s], got [$stage]"
                )
                acc
              case None =>
                acc.updated(groupId, stage)
            }
        }
        new CommittableOffsetBatchImpl(newOffsetsAndMetadata, newStages)
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffsetBatch, got [${committableOffsetBatch.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
        )
    }

  override def getOffsets(): JMap[GroupTopicPartition, Long] =
    offsets.asJava

  override def toString(): String =
    s"CommittableOffsetBatch(${offsets.mkString("->")})"

  override def commitScaladsl(): Future[Done] =
    if (offsets.isEmpty)
      Future.successful(Done)
    else {
      stages.head._2.commit(this)
    }

  override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava

}
