/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.concurrent.CompletionStage
import java.util.{Map => JMap}

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
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

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters.FutureOps
import scala.concurrent.{ExecutionContext, Future}

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
  def committer: InternalCommitter
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

/** Internal API */
@InternalApi private[kafka] final case class CommittableOffsetImpl(
    override val partitionOffset: ConsumerMessage.PartitionOffset,
    override val metadata: String
)(
    val committer: InternalCommitter
) extends CommittableOffsetMetadata {
  override def commitScaladsl(): Future[Done] = {
    val offset = partitionOffset.withMetadata(metadata)
    committer.commit(
      Map(
        new TopicPartition(offset.key.topic, offset.key.partition) ->
        new OffsetAndMetadata(offset.offset + 1, offset.metadata)
      )
    )
  }

  override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava
  override val batchSize: Long = 1
}

/** Internal API */
@InternalApi
private[kafka] trait InternalCommitter {
  // Commit all offsets (of different topics) belonging to the same stage
  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Done]
}

/** Internal API */
@InternalApi
private[kafka] final class CommittableOffsetBatchImpl(
    val committers: Map[String, (InternalCommitter, Map[TopicPartition, OffsetAndMetadata])],
    override val batchSize: Long
) extends CommittableOffsetBatch {

  def offsets(): Map[GroupTopicPartition, Long] = committers.flatMap {
    case (groupId, (_, offsetsMap)) =>
      offsetsMap.map {
        case (tp, om) => GroupTopicPartition(groupId, tp.topic, tp.partition) -> om.offset
      }
  }

  def updated(committable: Committable): CommittableOffsetBatch = committable match {
    case offset: CommittableOffset => updatedWithOffset(offset)
    case batch: CommittableOffsetBatch => updatedWithBatch(batch)
  }

  private def updatedWithOffset(committableOffset: CommittableOffset): CommittableOffsetBatch = {
    val key = committableOffset.partitionOffset.key
    val groupId = key.groupId
    val topicPartition = new TopicPartition(key.topic, key.partition)

    lazy val metadata = committableOffset match {
      case offset: CommittableOffsetMetadata => offset.metadata
      case _ => OffsetFetchResponse.NO_METADATA
    }

    lazy val committer = committableOffset match {
      case c: CommittableOffsetImpl => c.committer
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffset, got [${committableOffset.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
    }

    val newCommitters: Map[String, (InternalCommitter, Map[TopicPartition, OffsetAndMetadata])] = committers.updated(
      groupId,
      committers
        .get(groupId)
        .fold {
          val offsetsMap = Map(
            topicPartition -> new OffsetAndMetadata(committableOffset.partitionOffset.offset + 1, metadata)
          )
          (committer, offsetsMap)
        } {
          case (s, offsetsMap) =>
            require(
              s == committer,
              s"CommittableOffset [$committableOffset] committer for groupId [$groupId] " +
              s"must be same as the other with this groupId. Expected [$s], got [$committer]"
            )

            val offset = offsetsMap.get(topicPartition).fold(committableOffset.partitionOffset.offset + 1)(_.offset + 1)
            val updatedOffsetsMap = offsetsMap.updated(topicPartition, new OffsetAndMetadata(offset, metadata))
            (committer, updatedOffsetsMap)
        }
    )

    new CommittableOffsetBatchImpl(newCommitters, batchSize + 1)
  }

  private def updatedWithBatch(committableOffsetBatch: CommittableOffsetBatch): CommittableOffsetBatch =
    committableOffsetBatch match {
      case c: CommittableOffsetBatchImpl =>
        val newCommitters = c.committers.foldLeft(committers) {
          case (committersAcc, (groupId, (committer, newOffsetsMap))) =>
            committersAcc.updated(
              groupId,
              committersAcc.get(groupId).fold((committer, newOffsetsMap)) {
                case (s, oldOffsetsMap) =>
                  require(
                    s == committer,
                    s"CommittableOffsetBatch [$committableOffsetBatch] committer for groupId [$groupId] " +
                    s"must be same as the other with this groupId. Expected [$s], got [$committer]"
                  )

                  val offsetsMap = newOffsetsMap.foldLeft(oldOffsetsMap) {
                    case (offsetsAcc, (topicPartition, newOffsetAndMetadata)) =>
                      offsetsAcc.updated(
                        topicPartition,
                        offsetsAcc.get(topicPartition).fold(newOffsetAndMetadata) { oldOffsetAndMetadata =>
                          new OffsetAndMetadata(math.max(oldOffsetAndMetadata.offset, newOffsetAndMetadata.offset),
                                                newOffsetAndMetadata.metadata)
                        }
                      )
                  }

                  (committer, offsetsMap)
              }
            )
        }
        new CommittableOffsetBatchImpl(newCommitters, batchSize + committableOffsetBatch.batchSize)

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
    if (committers.isEmpty) Future.successful(Done)
    else {
      implicit def ec: ExecutionContext = ExecutionContexts.sameThreadExecutionContext

      Future.traverse(committers.values) { case (committer, offsets) => committer.commit(offsets) }.map(_ => Done)
    }

  override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava

}
