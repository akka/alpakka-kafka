/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import akka.actor.ActorRef
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.internal.{CommittingStage, ConsumerStage, PlainSource}
import akka.kafka.{AutoSubscription, ConsumerSettings, ManualSubscription, Subscription}
import akka.stream.scaladsl.{Flow, Source}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Akka Stream connector for subscribing to Kafka topics.
 */
object Consumer {

  /**
   * Materialized value of the consumer `Source`.
   */
  trait Control {

    /**
     * Stop producing messages from the `Source`. This does not stop the underlying kafka consumer
     * and does not unsubscribe from any topics/partitions.
     *
     * Call [[#shutdown]] to close consumer.
     */
    def stop(): Future[Done]

    /**
     * Shutdown the consumer `Source`. It will wait for outstanding offset
     * commit requests to finish before shutting down.
     */
    def shutdown(): Future[Done]

    /**
     * Shutdown status. The `Future` will be completed when the stage has been shut down
     * and the underlying `KafkaConsumer` has been closed. Shutdown can be triggered
     * from downstream cancellation, errors, or [[#shutdown]].
     */
    def isShutdown: Future[Done]
  }

  /**
   * The `plainSource` emits `ConsumerRecord` elements (as received from the underlying `KafkaConsumer`).
   * It has no support for committing offsets to Kafka. It can be used when the offset is stored externally
   * or with auto-commit (note that auto-commit is by default disabled).
   *
   * The consumer application doesn't need to use Kafka's built-in offset storage and can store offsets in a store of its own
   * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
   * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
   * possible, but when it is, it will make the consumption fully atomic and give "exactly once" semantics that are
   * stronger than the "at-least once" semantics you get with Kafka's offset commit functionality.
   */
  def plainSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[ConsumerRecord[K, V], Control] =
    Source.fromGraph(ConsumerStage.plainSource[K, V](settings, subscription))

  /**
   * The `plainSource` emits `ConsumerRecord` elements (as received from the underlying `KafkaConsumer`).
   * It has no support for committing offsets to Kafka. It can be used when the offset is stored externally
   * or with auto-commit (note that auto-commit is by default disabled).
   *
   * The consumer application doesn't need to use Kafka's built-in offset storage and can store offsets in a store of its own
   * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
   * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
   * possible, but when it is, it will make the consumption fully atomic and give "exactly once" semantics that are
   * stronger than the "at-least once" semantics you get with Kafka's offset commit functionality.
   */
  def plainSourceStage[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[ConsumerRecord[K, V], Control] = {
    PlainSource(settings, subscription).mapMaterializedValue(_ => new Control {
      override def stop(): Future[Done] = Future.successful(Done)
      override def shutdown(): Future[Done] = Future.successful(Done)
      override def isShutdown: Future[Done] = Future.successful(Done)
    })
  }

  /**
   * The `committingSource` takes a flow which all records are routed through and commits all records that have successfully been
   * processed by the flow.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * If you need to store offsets in anything other than Kafka, [[#plainSource]] should be used
   * instead of this API.
   */
  def committingSource[K, V](
    settings: ConsumerSettings[K, V],
    subscription: Subscription,
    flow: Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], NotUsed],
    commitInterval: Int = 1
  ): Source[ConsumerRecord[K, V], Control] = {

    CommittingStage(settings, subscription, flow, commitInterval).mapMaterializedValue(_ => new Control {
      override def stop(): Future[Done] = Future.successful(Done)
      override def shutdown(): Future[Done] = Future.successful(Done)
      override def isShutdown: Future[Done] = Future.successful(Done)
    })
  }

  /**
   * The `committableSource` makes it possible to commit offset positions to Kafka.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * If you commit the offset before processing the message you get "at-most once delivery" semantics,
   * and for that there is a [[#atMostOnceSource]].
   *
   * Compared to auto-commit, this gives exact control over when a message is considered consumed.
   *
   * If you need to store offsets in anything other than Kafka, [[#plainSource]] should be used
   * instead of this API.
   */
  def committableSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[CommittableMessage[K, V], Control] =
    Source.fromGraph(ConsumerStage.committableSource[K, V](settings, subscription))

  /**
   * Convenience for "at-most once delivery" semantics. The offset of each message is committed to Kafka
   * before it is emitted downstream.
   */
  def atMostOnceSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[ConsumerRecord[K, V], Control] = {
    committableSource[K, V](settings, subscription).mapAsync(1) { m =>
      m.committableOffset.commitScaladsl().map(_ => m.record)(ExecutionContexts.sameThreadExecutionContext)
    }
  }

  /**
   * The `plainPartitionedSource` is a way to track automatic partition assignment from kafka.
   * When a topic-partition is assigned to a consumer, this source will emit tuples with the assigned topic-partition and a corresponding
   * source of `ConsumerRecord`s.
   * When a topic-partition is revoked, the corresponding source completes.
   */
  def plainPartitionedSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription): Source[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed]), Control] =
    Source.fromGraph(ConsumerStage.plainSubSource[K, V](settings, subscription))

  /**
   * The `plainPartitionedManualOffsetSource` is similar to [[#plainPartitionedSource]] but allows the use of an offset store outside
   * of Kafka, while retaining the automatic partition assignment. When a topic-partition is assigned to a consumer, the `loadOffsetsOnAssign`
   * function will be called to retrieve the offset, followed by a seek to the correct spot in the partition. The `onRevoke` function gives
   * the consumer a chance to store any uncommitted offsets, and do any other cleanup that is required. Also allows the user access to the
   * `onPartitionsRevoked` hook, useful for cleaning up any partition-specific resources being used by the consumer.
   */
  def plainPartitionedManualOffsetSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription, getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]], onRevoke: Set[TopicPartition] => Unit = _ => ()): Source[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed]), Control] =
    Source.fromGraph(ConsumerStage.plainSubSource[K, V](settings, subscription, Some(getOffsetsOnAssign), onRevoke))

  /**
   * The same as [[#plainPartitionedSource]] but with offset commit support
   */
  def committablePartitionedSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription): Source[(TopicPartition, Source[CommittableMessage[K, V], NotUsed]), Control] =
    Source.fromGraph(ConsumerStage.committableSubSource[K, V](settings, subscription))

  /**
   * Special source that can use an external `KafkaAsyncConsumer`. This is useful when you have
   * a lot of manually assigned topic-partitions and want to keep only one kafka consumer.
   */
  def plainExternalSource[K, V](consumer: ActorRef, subscription: ManualSubscription): Source[ConsumerRecord[K, V], Control] = {
    Source.fromGraph(ConsumerStage.externalPlainSource[K, V](consumer, subscription))
  }

  /**
   * The same as [[#plainExternalSource]] but with offset commit support.
   */
  def committableExternalSource[K, V](consumer: ActorRef, subscription: ManualSubscription, groupId: String, commitTimeout: FiniteDuration): Source[CommittableMessage[K, V], Control] = {
    Source.fromGraph(ConsumerStage.externalCommittableSource[K, V](
      consumer, groupId, commitTimeout, subscription
    ))
  }
}
