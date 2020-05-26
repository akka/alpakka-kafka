/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
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
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.OffsetFetchResponse

import scala.collection.compat._
import scala.compat.java8.FutureConverters.FutureOps
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

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

  def fromPartitionedSource: Boolean

  val onTransactionAborted: Promise[Unit] = Promise()
  val onFirstMessageReceived: Promise[Unit] = Promise()

  def initCallbacks(firstMessageReceivedCb: AsyncCallback[Unit],
                    transactionAbortedCb: AsyncCallback[Unit])(implicit ec: ExecutionContext): Unit = {
    onFirstMessageReceived.future.onComplete {
      case Success(_) => firstMessageReceivedCb.invoke(())
      case Failure(ex) => throw new RuntimeException("The first message was not received", ex)
    }
    onTransactionAborted.future.onComplete {
      case Success(_) => transactionAbortedCb.invoke(())
      case Failure(ex) => throw new RuntimeException("An error occurred while during the transaction abort process", ex)
    }
  }
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
      committedMarker,
      fromPartitionedSource
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
      committedMarker,
      fromPartitionedSource
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
  override def commitScaladsl(): Future[Done] = commitInternal()
  override def commitJavadsl(): CompletionStage[Done] = commitInternal().toJava
  override def commitInternal(): Future[Done] = committer.commitSingle(this)
  override val batchSize: Long = 1
}

/** Internal API */
@InternalApi
private[kafka] trait CommittedMarker {

  /** Marks offsets as already committed */
  def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Done]

  /** Marks commit aborted */
  def onTransactionAborted: Promise[Unit]

  /** Marks first message enlisted in transaction */
  def onFirstMessageReceived: Promise[Unit]

}

/** Internal API */
@InternalApi
private[kafka] final class CommittableOffsetBatchImpl(
    private[kafka] val offsetsAndMetadata: Map[GroupTopicPartition, OffsetAndMetadata],
    private val committers: Map[GroupTopicPartition, KafkaAsyncConsumerCommitterRef],
    override val batchSize: Long
) extends CommittableOffsetBatch {
  def offsets: Map[GroupTopicPartition, Long] = offsetsAndMetadata.view.mapValues(_.offset() - 1L).toMap

  def updated(committable: Committable): CommittableOffsetBatch = committable match {
    case offset: CommittableOffset => updatedWithOffset(offset)
    case batch: CommittableOffsetBatch => updatedWithBatch(batch)
  }

  private[internal] def committerFor(groupTopicPartition: GroupTopicPartition) =
    committers.getOrElse(
      groupTopicPartition,
      throw new IllegalStateException(s"Unknown committer, got [$groupTopicPartition] (${committers.keys})")
    )

  private def updatedWithOffset(newOffset: CommittableOffset): CommittableOffsetBatch = {
    val partitionOffset = newOffset.partitionOffset
    val key = partitionOffset.key
    val metadata = newOffset match {
      case offset: CommittableOffsetMetadata =>
        offset.metadata
      case _ =>
        OffsetFetchResponse.NO_METADATA
    }

    val newOffsets =
      offsetsAndMetadata.updated(key, new OffsetAndMetadata(newOffset.partitionOffset.offset + 1L, metadata))

    val newCommitter = newOffset match {
      case c: CommittableOffsetImpl => c.committer
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffset, got [${newOffset.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
    }

    // the last `KafkaAsyncConsumerCommitterRef` wins (see https://github.com/akka/alpakka-kafka/issues/942)
    val newCommitters = committers.updated(key, newCommitter)
    new CommittableOffsetBatchImpl(newOffsets, newCommitters, batchSize + 1)
  }

  private def updatedWithBatch(committableOffsetBatch: CommittableOffsetBatch): CommittableOffsetBatch =
    committableOffsetBatch match {
      case newBatch: CommittableOffsetBatchImpl =>
        val newOffsetsAndMetadata = offsetsAndMetadata ++ newBatch.offsetsAndMetadata
        // the last `KafkaAsyncConsumerCommitterRef` wins (see https://github.com/akka/alpakka-kafka/issues/942)
        val newCommitters = committers ++ newBatch.committers
        new CommittableOffsetBatchImpl(newOffsetsAndMetadata, newCommitters, batchSize + newBatch.batchSize)
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffsetBatch, got [${committableOffsetBatch.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
        )
    }

  override def getOffsets(): java.util.Map[GroupTopicPartition, Long] =
    offsets.asJava

  override def toString: String =
    s"CommittableOffsetBatch(batchSize=$batchSize, ${offsets.mkString(", ")})"

  override def commitScaladsl(): Future[Done] = commitInternal()

  override def commitInternal(): Future[Done] =
    if (isEmpty)
      Future.successful(Done)
    else {
      committers.head._2.commit(this)
    }

  override def tellCommit(): CommittableOffsetBatch = tellCommitWithPriority(emergency = false)

  override def tellCommitEmergency(): CommittableOffsetBatch = tellCommitWithPriority(emergency = true)

  private def tellCommitWithPriority(emergency: Boolean): CommittableOffsetBatch = {
    if (batchSize != 0L) {
      committers.head._2.tellCommit(this, emergency = emergency)
    }
    this
  }

  override def commitJavadsl(): CompletionStage[Done] = commitInternal().toJava

  /**
   * @return true if the batch contains no commits.
   */
  def isEmpty: Boolean = batchSize == 0
}
