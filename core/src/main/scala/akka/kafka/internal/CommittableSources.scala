/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch, PartitionOffsetMetadata}
import akka.kafka._
import akka.kafka.internal.KafkaConsumerActor.Internal.Commit
import akka.kafka.scaladsl.Consumer.Control
import akka.pattern.AskTimeoutException
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.OffsetFetchResponse

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/** Internal API */
@InternalApi
private[kafka] final class CommittableSource[K, V](settings: ConsumerSettings[K, V],
                                                   subscription: Subscription,
                                                   _metadataFromRecord: ConsumerRecord[K, V] => String =
                                                     (_: ConsumerRecord[K, V]) => OffsetFetchResponse.NO_METADATA)
    extends KafkaSourceStage[K, V, CommittableMessage[K, V]](
      s"CommittableSource ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(shape: SourceShape[CommittableMessage[K, V]]): GraphStageLogic with Control =
    new SingleSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription)
    with CommittableMessageBuilder[K, V] {
      override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
      override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
      lazy val committer: Committer = {
        val ec = materializer.executionContext
        KafkaAsyncConsumerCommitterRef(consumerActor, settings.commitTimeout)(ec)
      }
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
      lazy val committer: Committer = {
        val ec = materializer.executionContext
        KafkaAsyncConsumerCommitterRef(consumerActor, commitTimeout)(ec)
      }
    }
}

/** Internal API */
@InternalApi
private[kafka] final class CommittableSubSource[K, V](settings: ConsumerSettings[K, V],
                                                      subscription: AutoSubscription,
                                                      _metadataFromRecord: ConsumerRecord[K, V] => String =
                                                        (_: ConsumerRecord[K, V]) => OffsetFetchResponse.NO_METADATA)
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
      lazy val committer: Committer = {
        val ec = materializer.executionContext
        KafkaAsyncConsumerCommitterRef(consumerActor, settings.commitTimeout)(ec)
      }
    }
}

/**
 * Internal API.
 *
 * [[Committer]] implementation for committable sources.
 *
 * Sends [[akka.kafka.internal.KafkaConsumerActor.Internal.Commit Commit]] messages to the consumer actor.
 *
 * This should be case class to be comparable based on consumerActor and commitTimeout. This comparison is used in [[CommittableOffsetBatchImpl]].
 */
private final case class KafkaAsyncConsumerCommitterRef(consumerActor: ActorRef, commitTimeout: FiniteDuration)(
    implicit ec: ExecutionContext
) extends Committer {
  import akka.pattern.ask
  import scala.collection.breakOut

  override def commit(offsets: immutable.Seq[PartitionOffsetMetadata]): Future[Done] = {
    val offsetsMap: Map[TopicPartition, OffsetAndMetadata] = offsets.map { offset =>
      new TopicPartition(offset.key.topic, offset.key.partition) ->
      new OffsetAndMetadata(offset.offset + 1, offset.metadata)
    }(breakOut)

    consumerActor
      .ask(Commit(offsetsMap))(Timeout(commitTimeout))
      .map(_ => Done)
      .recoverWith {
        case _: AskTimeoutException =>
          Future.failed(new CommitTimeoutException(s"Kafka commit took longer than: $commitTimeout"))
        case other => Future.failed(other)
      }
  }

  override def commit(batch: CommittableOffsetBatch): Future[Done] = batch match {
    case b: CommittableOffsetBatchImpl =>
      val futures = b.offsetsAndMetadata.groupBy(_._1.groupId).map {
        case (groupId, offsetsMap) =>
          val committer = b.stages.getOrElse(
            groupId,
            throw new IllegalStateException(s"Unknown committer, got [$groupId]")
          )
          val offsets: immutable.Seq[PartitionOffsetMetadata] = offsetsMap.map {
            case (ctp, offset) => PartitionOffsetMetadata(ctp, offset.offset(), offset.metadata())
          }(breakOut)
          committer.commit(offsets)
      }
      Future.sequence(futures).map(_ => Done)

    case _ =>
      throw new IllegalArgumentException(
        s"Unknown CommittableOffsetBatch, got [${batch.getClass.getName}], " +
        s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
      )
  }
}
