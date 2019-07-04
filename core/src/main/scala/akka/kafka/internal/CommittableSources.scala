/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.NotUsed
import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.{
  CommittableMessage,
  CommittableOffset,
  CommittableOffsetBatch,
  PartitionOffsetMetadata
}
import akka.kafka._
import akka.kafka.internal.KafkaConsumerActor.Internal.Commit
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.OffsetFetchResponse

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

/** Internal API */
@InternalApi
private[kafka] final class CommittableSource[K, V](settings: ConsumerSettings[K, V],
                                                   subscription: Subscription,
                                                   _metadataFromRecord: ConsumerRecord[K, V] => String =
                                                     CommittableMessageBuilder.NoMetadataFromRecord)
    extends KafkaSourceStage[K, V, CommittableMessage[K, V]](
      s"CommittableSource ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(shape: SourceShape[CommittableMessage[K, V]]): GraphStageLogic with Control =
    new SingleSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription)
    with CommittableMessageBuilder[K, V] {
      override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
      override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
      lazy val committer: InternalCommitter = new KafkaAsyncConsumerCommitterRef(consumerActor)
    }
}

/** Internal API */
@InternalApi
private[kafka] final class SourceWithOffsetContext[K, V](
    settings: ConsumerSettings[K, V],
    subscription: Subscription,
    _metadataFromRecord: ConsumerRecord[K, V] => String = CommittableMessageBuilder.NoMetadataFromRecord
) extends KafkaSourceStage[K, V, (ConsumerRecord[K, V], CommittableOffset)](
      s"SourceWithOffsetContext ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(
      shape: SourceShape[(ConsumerRecord[K, V], CommittableOffset)]
  ): GraphStageLogic with Control =
    new SingleSourceLogic[K, V, (ConsumerRecord[K, V], CommittableOffset)](shape, settings, subscription)
    with OffsetContextBuilder[K, V] {
      override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
      override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
      lazy val committer: InternalCommitter = new KafkaAsyncConsumerCommitterRef(consumerActor)
    }
}

/** Internal API */
@InternalApi
private[kafka] final class ExternalCommittableSource[K, V](consumer: ActorRef,
                                                           _groupId: String,
                                                           commitTimeout: FiniteDuration,
                                                           subscription: ManualSubscription)
    extends KafkaSourceStage[K, V, CommittableMessage[K, V]](
      s"ExternalCommittableSource ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(shape: SourceShape[CommittableMessage[K, V]]): GraphStageLogic with Control =
    new ExternalSingleSourceLogic[K, V, CommittableMessage[K, V]](shape, consumer, subscription)
    with CommittableMessageBuilder[K, V] {
      override def metadataFromRecord(record: ConsumerRecord[K, V]): String = OffsetFetchResponse.NO_METADATA
      override def groupId: String = _groupId
      lazy val committer: InternalCommitter = new KafkaAsyncConsumerCommitterRef(consumerActor)
    }
}

/** Internal API */
@InternalApi
private[kafka] final class CommittableSubSource[K, V](settings: ConsumerSettings[K, V],
                                                      subscription: AutoSubscription,
                                                      _metadataFromRecord: ConsumerRecord[K, V] => String =
                                                        CommittableMessageBuilder.NoMetadataFromRecord)
    extends KafkaSourceStage[K, V, (TopicPartition, Source[CommittableMessage[K, V], NotUsed])](
      s"CommittableSubSource ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(
      shape: SourceShape[(TopicPartition, Source[CommittableMessage[K, V], NotUsed])]
  ): GraphStageLogic with Control =
    new SubSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription)
    with CommittableMessageBuilder[K, V] with MetricsControl {
      override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
      override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
      lazy val committer: InternalCommitter = new KafkaAsyncConsumerCommitterRef(consumerActor)
    }
}

/**
 * Internal API.
 *
 * [[InternalCommitter]] implementation for committable sources.
 *
 * Sends [[akka.kafka.internal.KafkaConsumerActor.Internal.Commit Commit]] messages to the consumer actor.
 */
private final class KafkaAsyncConsumerCommitterRef(consumerActor: ActorRef) extends InternalCommitter {

  override def commit(offsets: immutable.Seq[PartitionOffsetMetadata]): Unit = {
    val offsetsMap: Map[TopicPartition, OffsetAndMetadata] = getOffsetsMap(offsets)
    consumerActor ! Commit(offsetsMap)
  }

  override def commit(batch: CommittableOffsetBatch): Unit = batch match {
    case b: CommittableOffsetBatchImpl =>
      groupBatch(b).foreach { case (committer, offsets) => committer.commit(offsets) }
    case _ =>
      throw new IllegalArgumentException(
        s"Unknown CommittableOffsetBatch, got [${batch.getClass.getName}], " +
        s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
      )
  }

  private def getOffsetsMap(offsets: immutable.Seq[PartitionOffsetMetadata]): Map[TopicPartition, OffsetAndMetadata] =
    offsets.map { offset =>
      new TopicPartition(offset.key.topic, offset.key.partition) ->
      new OffsetAndMetadata(offset.offset + 1, offset.metadata)
    }.toMap

  private def groupBatch(
      b: CommittableOffsetBatchImpl
  ): Map[InternalCommitter, immutable.Seq[PartitionOffsetMetadata]] =
    b.offsetsAndMetadata
      .groupBy { case (groupTp, _) => groupTp.groupId }
      .map {
        case (groupId, offsetsMap) =>
          val committer = b.committers.getOrElse(
            groupId,
            throw new IllegalStateException(s"Unknown committer, got [$groupId]")
          )
          val offsets: immutable.Seq[PartitionOffsetMetadata] = offsetsMap.map {
            case (ctp, offset) => PartitionOffsetMetadata(ctp, offset.offset(), offset.metadata())
          }.toList
          (committer, offsets)
      }
}
