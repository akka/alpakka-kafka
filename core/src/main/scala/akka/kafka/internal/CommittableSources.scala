/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka._
import akka.kafka.internal.KafkaConsumerActor.Internal.{Commit, CommitSingle, CommitWithoutReply}
import akka.kafka.internal.SubSourceLogic._
import akka.kafka.scaladsl.Consumer.Control
import akka.pattern.AskTimeoutException
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
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
  ): GraphStageLogic with Control = {

    val factory = new SubSourceStageLogicFactory[K, V, CommittableMessage[K, V]] {
      def create(
          shape: SourceShape[CommittableMessage[K, V]],
          tp: TopicPartition,
          consumerActor: ActorRef,
          subSourceStartedCb: AsyncCallback[SubSourceStageLogicControl],
          subSourceCancelledCb: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)],
          actorNumber: Int
      ): SubSourceStageLogic[K, V, CommittableMessage[K, V]] =
        new CommittableSubSourceStageLogic(shape,
                                           tp,
                                           consumerActor,
                                           subSourceStartedCb,
                                           subSourceCancelledCb,
                                           actorNumber,
                                           settings,
                                           _metadataFromRecord)

    }
    new SubSourceLogic[K, V, CommittableMessage[K, V]](shape,
                                                       settings,
                                                       subscription,
                                                       getOffsetsOnAssign,
                                                       onRevoke,
                                                       subSourceStageLogicFactory = factory)
  }
}

@InternalApi
private[kafka] object KafkaAsyncConsumerCommitterRef {

  def commit(offset: CommittableOffsetImpl): Future[Done] = {
    offset.committer.commitSingle(
      new TopicPartition(offset.partitionOffset.key.topic, offset.partitionOffset.key.partition),
      new OffsetAndMetadata(offset.partitionOffset.offset + 1, offset.metadata)
    )
  }

  def commit(batch: CommittableOffsetBatchImpl): Future[Done] = {
    val futures = forBatch(batch) {
      case (committer, topicPartition, offset) =>
        committer.commitOneOfMulti(topicPartition, offset)
    }
    getFirstExecutionContext(batch)
      .map { implicit ec =>
        Future.sequence(futures).map(_ => Done)(ExecutionContexts.parasitic)
      }
      .getOrElse(Future.successful(Done))
  }

  def tellCommit(batch: CommittableOffsetBatchImpl, emergency: Boolean): Unit = {
    forBatch(batch) {
      case (committer, topicPartition, offset) =>
        committer.tellCommit(topicPartition, offset, emergency)
    }
  }

  private def getFirstExecutionContext(batch: CommittableOffsetBatchImpl): Option[ExecutionContext] = {
    batch.offsetsAndMetadata.keys.headOption.map { groupTopicPartition =>
      batch.committerFor(groupTopicPartition).ec
    }
  }

  private def forBatch[T](
      batch: CommittableOffsetBatchImpl
  )(sendMsg: (KafkaAsyncConsumerCommitterRef, TopicPartition, OffsetAndMetadata) => T) = {
    val results = batch.offsetsAndMetadata.map {
      case (groupTopicPartition, offset) =>
        // sends one message per partition, they are aggregated in the KafkaConsumerActor
        val committer = batch.committerFor(groupTopicPartition)
        sendMsg(committer, groupTopicPartition.topicPartition, offset)
    }
    results
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
    private val ec: ExecutionContext
) {
  def commitSingle(topicPartition: TopicPartition, offset: OffsetAndMetadata): Future[Done] = {
    sendWithReply(CommitSingle(topicPartition, offset))
  }

  def commitOneOfMulti(topicPartition: TopicPartition, offset: OffsetAndMetadata): Future[Done] = {
    sendWithReply(Commit(topicPartition, offset))
  }

  def tellCommit(topicPartition: TopicPartition, offset: OffsetAndMetadata, emergency: Boolean): Unit = {
    consumerActor ! CommitWithoutReply(topicPartition, offset, emergency)
  }

  private def sendWithReply(msg: AnyRef): Future[Done] = {
    import akka.pattern.ask
    consumerActor
      .ask(msg)(Timeout(commitTimeout))
      .map(_ => Done)(ExecutionContexts.parasitic)
      .recoverWith {
        case e: AskTimeoutException =>
          Future.failed(new CommitTimeoutException(s"Kafka commit took longer than: $commitTimeout (${e.getMessage})"))
        case other => Future.failed(other)
      }(ec)
  }

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

@InternalApi
private final class CommittableSubSourceStageLogic[K, V](
    shape: SourceShape[CommittableMessage[K, V]],
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[SubSourceStageLogicControl],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)],
    actorNumber: Int,
    consumerSettings: ConsumerSettings[K, V],
    _metadataFromRecord: ConsumerRecord[K, V] => String = CommittableMessageBuilder.NoMetadataFromRecord
) extends SubSourceStageLogic[K, V, CommittableMessage[K, V]](shape,
                                                                tp,
                                                                consumerActor,
                                                                subSourceStartedCb,
                                                                subSourceCancelledCb,
                                                                actorNumber)
    with CommittableMessageBuilder[K, V] {

  override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
  override def groupId: String = consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)
  lazy val committer: KafkaAsyncConsumerCommitterRef = {
    val ec = materializer.executionContext
    new KafkaAsyncConsumerCommitterRef(consumerActor, consumerSettings.commitTimeout)(ec)
  }
}
