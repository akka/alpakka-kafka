/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorRef
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka._
import akka.kafka.internal._
import akka.stream.scaladsl.{Source, SourceWithContext}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
 * Akka Stream connector for subscribing to Kafka topics.
 */
object Consumer {

  /**
   * Materialized value of the consumer `Source`.
   *
   * See [[https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#controlled-shutdown Controlled shutdown]]
   */
  trait Control {

    /**
     * Stop producing messages from the `Source` and complete the stream.
     * The underlying Kafka consumer stays alive so that it can handle commits for the
     * already enqueued messages. It does not unsubscribe from any topics/partitions
     * as that could trigger a consumer group rebalance.
     *
     * See [[https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#controlled-shutdown Controlled shutdown]]
     *
     * Call [[#shutdown]] to close consumer.
     */
    def stop(): Future[Done]

    /**
     * Shut down the consumer `Source`.
     *
     * The actor backing the source will stay alive for `akka.kafka.consumer.stop-timeout` so that more commits
     * from enqueued messages can be handled.
     * The actor will wait for acknowledgements of the already sent offset commits from the Kafka broker before shutting down.
     *
     * See [[https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#controlled-shutdown Controlled shutdown]]
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
        .recoverWith {
          case completionError: Throwable =>
            shutdown()
              .flatMap(_ => streamCompletion)
              .recoverWith {
                case _: Throwable => throw completionError
              }
        }
        .flatMap { result =>
          shutdown()
            .map(_ => result)
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
   *
   * See [[https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#controlled-shutdown Controlled shutdown]]
   */
  final class DrainingControl[T] private (control: Control, val streamCompletion: Future[T]) extends Control {

    override def stop(): Future[Done] = control.stop()

    @deprecated("Use `drainAndShutdown` for proper shutdown of the stream.", "2.0.0")
    override def shutdown(): Future[Done] =
      control
        .shutdown()
        .flatMap(_ => streamCompletion)(ExecutionContexts.parasitic)
        .map(_ => Done)(ExecutionContexts.parasitic)

    override def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S] =
      control.drainAndShutdown(streamCompletion)

    /**
     * Stop producing messages from the `Source`, wait for stream completion
     * and shut down the consumer `Source` so that all consumed messages
     * reach the end of the stream.
     */
    def drainAndShutdown()(implicit ec: ExecutionContext): Future[T] = control.drainAndShutdown(streamCompletion)(ec)

    override val isShutdown: Future[Done] =
      control.isShutdown
        .flatMap(_ => streamCompletion)(ExecutionContexts.parasitic)
        .map(_ => Done)(ExecutionContexts.parasitic)

    override def metrics: Future[Map[MetricName, Metric]] = control.metrics
  }

  object DrainingControl {

    /**
     * Combine the consumer control and a stream completion signal materialized values into
     * one, so that the stream can be stopped in a controlled way without losing
     * commits.
     *
     * For use in `mapMaterializedValue`.
     */
    def apply[T](tuple: (Control, Future[T])) = new DrainingControl[T](tuple._1, tuple._2)

    /**
     * Combine the consumer control and a stream completion signal materialized values into
     * one, so that the stream can be stopped in a controlled way without losing
     * commits.
     *
     * For use in the `toMat` combination of materialized values.
     */
    def apply[T]: (Control, Future[T]) => DrainingControl[T] = new DrainingControl[T](_, _)
  }

  /**
   * An implementation of Control to be used as an empty value, all methods return
   * a failed future.
   */
  object NoopControl extends Control {
    private def exception = new RuntimeException("The correct Consumer.Control has not been assigned, yet.")

    override def stop(): Future[Done] = Future.failed(exception)
    override def shutdown(): Future[Done] = Future.failed(exception)
    override def isShutdown: Future[Done] = Future.failed(exception)
    override def metrics: Future[Map[MetricName, Metric]] = Future.failed(exception)
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
  def plainSource[K, V](settings: ConsumerSettings[K, V],
                        subscription: Subscription): Source[ConsumerRecord[K, V], Control] =
    Source.fromGraph(new PlainSource[K, V](settings, subscription))

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
  def committableSource[K, V](settings: ConsumerSettings[K, V],
                              subscription: Subscription): Source[CommittableMessage[K, V], Control] =
    Source.fromGraph(new CommittableSource[K, V](settings, subscription))

  /**
   * API MAY CHANGE
   *
   * This source emits `ConsumerRecord` together with the offset position as flow context, thus makes it possible
   * to commit offset positions to Kafka.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * It is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html),
   * [[Producer.flowWithContext]] and/or [[Committer.sinkWithOffsetContext]].
   */
  @ApiMayChange
  def sourceWithOffsetContext[K, V](
      settings: ConsumerSettings[K, V],
      subscription: Subscription
  ): SourceWithContext[ConsumerRecord[K, V], CommittableOffset, Control] =
    Source
      .fromGraph(new SourceWithOffsetContext[K, V](settings, subscription))
      .asSourceWithContext(_._2)
      .map(_._1)

  /**
   * API MAY CHANGE
   *
   * This source emits `ConsumerRecord` together with the offset position as flow context, thus makes it possible
   * to commit offset positions to Kafka.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * It is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html),
   * [[Producer.flowWithContext]] and/or [[Committer.sinkWithOffsetContext]].
   *
   * This variant makes it possible to add additional metadata (in the form of a string)
   * when an offset is committed based on the record. This can be useful (for example) to store information about which
   * node made the commit, what time the commit was made, the timestamp of the record etc.
   */
  @ApiMayChange
  def sourceWithOffsetContext[K, V](
      settings: ConsumerSettings[K, V],
      subscription: Subscription,
      metadataFromRecord: ConsumerRecord[K, V] => String
  ): SourceWithContext[ConsumerRecord[K, V], CommittableOffset, Control] =
    Source
      .fromGraph(new SourceWithOffsetContext[K, V](settings, subscription, metadataFromRecord))
      .asSourceWithContext(_._2)
      .map(_._1)

  /**
   * The `commitWithMetadataSource` makes it possible to add additional metadata (in the form of a string)
   * when an offset is committed based on the record. This can be useful (for example) to store information about which
   * node made the commit, what time the commit was made, the timestamp of the record etc.
   */
  def commitWithMetadataSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: Subscription,
      metadataFromRecord: ConsumerRecord[K, V] => String
  ): Source[CommittableMessage[K, V], Control] =
    Source.fromGraph(new CommittableSource[K, V](settings, subscription, metadataFromRecord))

  /**
   * Convenience for "at-most once delivery" semantics. The offset of each message is committed to Kafka
   * before being emitted downstream.
   */
  def atMostOnceSource[K, V](settings: ConsumerSettings[K, V],
                             subscription: Subscription): Source[ConsumerRecord[K, V], Control] =
    committableSource[K, V](settings, subscription).mapAsync(1) { m =>
      m.committableOffset.commitInternal().map(_ => m.record)(ExecutionContexts.parasitic)
    }

  /**
   * The `plainPartitionedSource` is a way to track automatic partition assignment from kafka.
   * When a topic-partition is assigned to a consumer, this source will emit tuples with the assigned topic-partition and a corresponding
   * source of `ConsumerRecord`s.
   * When a topic-partition is revoked, the corresponding source completes.
   */
  def plainPartitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription
  ): Source[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed]), Control] =
    Source.fromGraph(new PlainSubSource[K, V](settings, subscription, None, onRevoke = _ => ()))

  /**
   * The `plainPartitionedManualOffsetSource` is similar to [[#plainPartitionedSource]] but allows the use of an offset store outside
   * of Kafka, while retaining the automatic partition assignment. When a topic-partition is assigned to a consumer, the `getOffsetsOnAssign`
   * function will be called to retrieve the offset, followed by a seek to the correct spot in the partition.
   *
   * The `onRevoke` function gives the consumer a chance to store any uncommitted offsets, and do any other cleanup
   * that is required. Also allows the user access to the `onPartitionsRevoked` hook, useful for cleaning up any
   * partition-specific resources being used by the consumer.
   */
  def plainPartitionedManualOffsetSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription,
      getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]],
      onRevoke: Set[TopicPartition] => Unit = _ => ()
  ): Source[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed]), Control] =
    Source.fromGraph(new PlainSubSource[K, V](settings, subscription, Some(getOffsetsOnAssign), onRevoke))

  /**
   * The same as [[#plainPartitionedManualOffsetSource]] but with offset commit support.
   */
  def committablePartitionedManualOffsetSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription,
      getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]],
      onRevoke: Set[TopicPartition] => Unit = _ => ()
  ): Source[(TopicPartition, Source[CommittableMessage[K, V], NotUsed]), Control] =
    Source.fromGraph(
      new CommittableSubSource[K, V](settings,
                                     subscription,
                                     getOffsetsOnAssign = Some(getOffsetsOnAssign),
                                     onRevoke = onRevoke)
    )

  /**
   * The same as [[#plainPartitionedSource]] but with offset commit support.
   */
  def committablePartitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription
  ): Source[(TopicPartition, Source[CommittableMessage[K, V], NotUsed]), Control] =
    Source.fromGraph(new CommittableSubSource[K, V](settings, subscription))

  /**
   * The same as [[#plainPartitionedSource]] but with offset commit with metadata support.
   */
  def commitWithMetadataPartitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription,
      metadataFromRecord: ConsumerRecord[K, V] => String
  ): Source[(TopicPartition, Source[CommittableMessage[K, V], NotUsed]), Control] =
    Source.fromGraph(new CommittableSubSource[K, V](settings, subscription, metadataFromRecord))

  /**
   * Special source that can use an external `KafkaAsyncConsumer`. This is useful when you have
   * a lot of manually assigned topic-partitions and want to keep only one kafka consumer.
   */
  def plainExternalSource[K, V](consumer: ActorRef,
                                subscription: ManualSubscription): Source[ConsumerRecord[K, V], Control] =
    Source.fromGraph(new ExternalPlainSource[K, V](consumer, subscription))

  /**
   * The same as [[#plainExternalSource]] but with offset commit support.
   */
  def committableExternalSource[K, V](consumer: ActorRef,
                                      subscription: ManualSubscription,
                                      groupId: String,
                                      commitTimeout: FiniteDuration): Source[CommittableMessage[K, V], Control] =
    Source.fromGraph(
      new ExternalCommittableSource[K, V](
        consumer,
        groupId,
        commitTimeout,
        subscription
      )
    )
}
