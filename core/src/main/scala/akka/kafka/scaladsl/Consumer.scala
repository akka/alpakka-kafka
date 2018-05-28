/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorRef
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.{CommittableMessage, TransactionalMessage}
import akka.kafka.internal.ConsumerStage
import akka.kafka._
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

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
     * Stop producing messages from the `Source`, wait for stream completion
     * and shut down the consumer `Source` so that all consumed messages
     * reach the end of the stream.
     * Failures in stream completion will be propagated, the source will be shut down anyway.
     */
    def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S] =
      stop()
        .flatMap(_ => streamCompletion)
        .transformWith { _ =>
          shutdown().flatMap(_ => streamCompletion)
        }

    /**
     * Shutdown status. The `Future` will be completed when the stage has been shut down
     * and the underlying `KafkaConsumer` has been closed. Shutdown can be triggered
     * from downstream cancellation, errors, or [[#shutdown]].
     */
    def isShutdown: Future[Done]

    /**
     * Exposes underlying consumer or producer metrics (as reported by underlying Kafka client library)
     */
    def metrics: Future[Map[MetricName, Metric]]
  }

  /**
   * Combine control and a stream completion signal materialized values into
   * one, so that the stream can be stopped in a controlled way without losing
   * commits.
   */
  final class DrainingControl[T] private (control: Control, streamCompletion: Future[T]) extends Control {

    override def stop(): Future[Done] = control.stop()

    override def shutdown(): Future[Done] = control.shutdown()

    override def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S] =
      control.drainAndShutdown(streamCompletion)

    /**
     * Stop producing messages from the `Source`, wait for stream completion
     * and shut down the consumer `Source` so that all consumed messages
     * reach the end of the stream.
     */
    def drainAndShutdown()(implicit ec: ExecutionContext): Future[T] = control.drainAndShutdown(streamCompletion)(ec)

    override def isShutdown: Future[Done] = control.isShutdown

    override def metrics: Future[Map[MetricName, Metric]] = control.metrics
  }

  object DrainingControl {
    /**
     * Combine control and a stream completion signal materialized values into
     * one, so that the stream can be stopped in a controlled way without losing
     * commits.
     */
    def apply[T](tuple: (Control, Future[T])) = new DrainingControl[T](tuple._1, tuple._2)
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
   * Transactional source to setup a stream for Exactly Only Once (EoS) kafka message semantics.  To enable EoS it's
   * necessary to use the [[Producer#transactionalSink]] or [[Producer#transactionalFlow]] (for passthrough).
   */
  def transactionalSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[TransactionalMessage[K, V], Control] =
    Source.fromGraph(ConsumerStage.transactionalSource[K, V](settings, subscription))

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
