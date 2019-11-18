/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset, CommittableOffsetBatch}
import akka.kafka._
import akka.kafka.internal.KafkaConsumerActor.Internal.{Commit, CommitSingle, CommitWithoutReply}
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

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

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
      lazy val committer: KafkaAsyncConsumerCommitterRef = {
        val ec = materializer.executionContext
        new KafkaAsyncConsumerCommitterRef(consumerActor, settings.commitTimeout)(ec)
      }
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
      lazy val committer: KafkaAsyncConsumerCommitterRef = {
        val ec = materializer.executionContext
        new KafkaAsyncConsumerCommitterRef(consumerActor, settings.commitTimeout)(ec)
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
      lazy val committer: KafkaAsyncConsumerCommitterRef = {
        val ec = materializer.executionContext
        new KafkaAsyncConsumerCommitterRef(consumerActor, commitTimeout)(ec)
      }
    }
}

/** Internal API */
@InternalApi
private[kafka] final class CommittableSubSource[K, V](
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription,
    _metadataFromRecord: ConsumerRecord[K, V] => String = CommittableMessageBuilder.NoMetadataFromRecord,
    getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]] = None,
    onRevoke: Set[TopicPartition] => Unit = _ => ()
) extends KafkaSourceStage[K, V, (TopicPartition, Source[CommittableMessage[K, V], NotUsed])](
      s"CommittableSubSource ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(
      shape: SourceShape[(TopicPartition, Source[CommittableMessage[K, V], NotUsed])]
  ): GraphStageLogic with Control =
    new SubSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription, getOffsetsOnAssign, onRevoke)
      with CommittableMessageBuilder[K, V]
      with MetricsControl {
      override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
      override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
      lazy val committer: KafkaAsyncConsumerCommitterRef = {
        val ec = materializer.executionContext
        new KafkaAsyncConsumerCommitterRef(consumerActor, settings.commitTimeout)(ec)
      }
    }
}

/**
 * Internal API.
 *
 * Sends [[akka.kafka.internal.KafkaConsumerActor.Internal.Commit]],
 * [[akka.kafka.internal.KafkaConsumerActor.Internal.CommitSingle]] and
 * [[akka.kafka.internal.KafkaConsumerActor.Internal.CommitWithoutReply]] messages to the consumer actor.
 */
@InternalApi
private[kafka] class KafkaAsyncConsumerCommitterRef(private val consumerActor: ActorRef,
                                                    private val commitTimeout: FiniteDuration)(
    implicit ec: ExecutionContext
) {

  def commitSingle(offset: CommittableOffsetImpl): Future[Done] =
    sendCommit(
      CommitSingle(
        new TopicPartition(offset.partitionOffset.key.topic, offset.partitionOffset.key.partition),
        new OffsetAndMetadata(offset.partitionOffset.offset + 1, offset.metadata)
      )
    )

  def commit(batch: CommittableOffsetBatch): Future[Done] = batch match {
    case b: CommittableOffsetBatchImpl =>
      val futures = b.offsetsAndMetadata.map {
        case (groupTopicPartition, offset) =>
          // sends one message per partition, they are aggregated in the KafkaConsumerActor
          b.committerFor(groupTopicPartition).sendCommit(Commit(groupTopicPartition.topicPartition, offset))
      }
      Future.sequence(futures).map(_ => Done)

    case _ => failForUnexpectedImplementation(batch)
  }

  def tellCommit(batch: CommittableOffsetBatch): Unit = batch match {
    case b: CommittableOffsetBatchImpl =>
      b.offsetsAndMetadata.foreach {
        case (groupTopicPartition, offset) =>
          // sends one message per partition, they are aggregated in the KafkaConsumerActor
          b.committerFor(groupTopicPartition)
            .tellCommit(CommitWithoutReply(groupTopicPartition.topicPartition, offset))
      }

    case _ => failForUnexpectedImplementation(batch)
  }

  private def failForUnexpectedImplementation(batch: CommittableOffsetBatch) =
    throw new IllegalArgumentException(
      s"Unknown CommittableOffsetBatch, got [${batch.getClass.getName}], " +
      s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
    )

  private def sendCommit(msg: AnyRef): Future[Done] = {
    import akka.pattern.ask
    consumerActor
      .ask(msg)(Timeout(commitTimeout))
      .map(_ => Done)(ec)
      .recoverWith {
        case _: AskTimeoutException =>
          Future.failed(new CommitTimeoutException(s"Kafka commit took longer than: $commitTimeout"))
        case other => Future.failed(other)
      }(ec)
  }

  private def tellCommit(msg: CommitWithoutReply): Unit = consumerActor ! msg

  /**
   * This must be comparable based on `consumerActor` and `commitTimeout`. The comparison is used in [[CommittableOffsetBatchImpl]].
   * The comparison is mostly relevant when multiple sources share a consumer actor.
   */
  override def equals(obj: Any): Boolean =
    obj match {
      case that: KafkaAsyncConsumerCommitterRef =>
        this.consumerActor == that.consumerActor && this.commitTimeout == that.commitTimeout
      case _ => false
    }
}
