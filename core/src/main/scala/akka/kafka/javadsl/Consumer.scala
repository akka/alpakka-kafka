/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.{CompletionStage, Executor}

import akka.actor.ActorRef
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.japi.Pair
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka._
import akka.kafka.internal.ConsumerControlAsJava
import akka.stream.javadsl.{Source, SourceWithContext}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
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
     * Stop producing messages from the `Source`. This does not stop underlying kafka consumer
     * and does not unsubscribe from any topics/partitions.
     *
     * Call [[#shutdown]] to close consumer
     */
    def stop(): CompletionStage[Done]

    /**
     * Shutdown the consumer `Source`. It will wait for outstanding offset
     * commit requests before shutting down.
     */
    def shutdown(): CompletionStage[Done]

    /**
     * Stop producing messages from the `Source`, wait for stream completion
     * and shut down the consumer `Source` so that all consumed messages
     * reach the end of the stream.
     * Failures in stream completion will be propagated, the source will be shut down anyway.
     */
    def drainAndShutdown[T](streamCompletion: CompletionStage[T], ec: Executor): CompletionStage[T]

    /**
     * Shutdown status. The `CompletionStage` will be completed when the stage has been shut down
     * and the underlying `KafkaConsumer` has been closed. Shutdown can be triggered
     * from downstream cancellation, errors, or [[#shutdown]].
     */
    def isShutdown: CompletionStage[Done]

    /**
     * Exposes underlying consumer or producer metrics (as reported by underlying Kafka client library)
     */
    def getMetrics: CompletionStage[java.util.Map[MetricName, Metric]]

  }

  /**
   * Combine control and a stream completion signal materialized values into
   * one, so that the stream can be stopped in a controlled way without losing
   * commits.
   */
  final class DrainingControl[T] private[javadsl] (control: Control, streamCompletion: CompletionStage[T])
      extends Control {

    override def stop(): CompletionStage[Done] = control.stop()

    override def shutdown(): CompletionStage[Done] = control.shutdown()

    override def drainAndShutdown[S](streamCompletion: CompletionStage[S], ec: Executor): CompletionStage[S] =
      control.drainAndShutdown(streamCompletion, ec)

    /**
     * Stop producing messages from the `Source`, wait for stream completion
     * and shut down the consumer `Source`. It will wait for outstanding offset
     * commit requests to finish before shutting down.
     */
    def drainAndShutdown(ec: Executor): CompletionStage[T] =
      control.drainAndShutdown(streamCompletion, ec)

    override def isShutdown: CompletionStage[Done] = control.isShutdown

    override def getMetrics: CompletionStage[java.util.Map[MetricName, Metric]] = control.getMetrics
  }

  /**
   * Combine control and a stream completion signal materialized values into
   * one, so that the stream can be stopped in a controlled way without losing
   * commits.
   */
  def createDrainingControl[T](pair: Pair[Control, CompletionStage[T]]) =
    new DrainingControl[T](pair.first, pair.second)

  /**
   * An implementation of Control to be used as an empty value, all methods return
   * a failed `CompletionStage`.
   */
  def createNoopControl(): Control = new ConsumerControlAsJava(scaladsl.Consumer.NoopControl)

  /**
   * The `plainSource` emits `ConsumerRecord` elements (as received from the underlying `KafkaConsumer`).
   * It has not support for committing offsets to Kafka. It can be used when offset is stored externally
   * or with auto-commit (note that auto-commit is by default disabled).
   *
   * The consumer application doesn't need to use Kafka's built-in offset storage, it can store offsets in a store of its own
   * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
   * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
   * possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
   * stronger than the "at-least once" semantics you get with Kafka's offset commit functionality.
   */
  def plainSource[K, V](settings: ConsumerSettings[K, V],
                        subscription: Subscription): Source[ConsumerRecord[K, V], Control] =
    scaladsl.Consumer
      .plainSource(settings, subscription)
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * The `committableSource` makes it possible to commit offset positions to Kafka.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * If you commit the offset before processing the message you get "at-most once delivery" semantics,
   * and for that there is a [[#atMostOnceSource]].
   *
   * Compared to auto-commit this gives exact control of when a message is considered consumed.
   *
   * If you need to store offsets in anything other than Kafka, [[#plainSource]] should be used
   * instead of this API.
   */
  def committableSource[K, V](settings: ConsumerSettings[K, V],
                              subscription: Subscription): Source[CommittableMessage[K, V], Control] =
    scaladsl.Consumer
      .committableSource(settings, subscription)
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * API MAY CHANGE
   *
   * The `committableSourceWithContext` makes it possible to commit offset positions to Kafka.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * This source is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html)
   * and [[Producer.withContext]].
   */
  @ApiMayChange
  def committableSourceWithContext[K, V](
      settings: ConsumerSettings[K, V],
      subscription: Subscription
  ): SourceWithContext[ConsumerRecord[K, V], CommittableOffset, Control] =
    committableSourceWithContext(
      settings,
      subscription,
      new java.util.function.Function[ConsumerRecord[K, V], String]() {
        override def apply(v1: ConsumerRecord[K, V]): String = OffsetFetchResponse.NO_METADATA
      }
    )

  /**
   * API MAY CHANGE
   *
   * The `committableSourceWithContext` makes it possible to commit offset positions to Kafka.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * This source is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html)
   * and [[Producer.withContext]].
   *
   * This source makes it possible to add additional metadata (in the form of a string)
   * when an offset is committed based on the record. This can be useful (for example) to store information about which
   * node made the commit, what time the commit was made, the timestamp of the record etc.
   */
  @ApiMayChange
  def committableSourceWithContext[K, V](
      settings: ConsumerSettings[K, V],
      subscription: Subscription,
      metadataFromRecord: java.util.function.Function[ConsumerRecord[K, V], String]
  ): SourceWithContext[ConsumerRecord[K, V], CommittableOffset, Control] =
    // TODO this should use `committableSourceWithContext` but `mapMaterializedValue` is not available, yet
    // See https://github.com/akka/akka/issues/26836
    scaladsl.Consumer
      .commitWithMetadataSource(settings, subscription, (record: ConsumerRecord[K, V]) => metadataFromRecord(record))
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asSourceWithContext(msg => msg.committableOffset)
      .map(msg => msg.record)
      .asJava

  /**
   * The `commitWithMetadataSource` makes it possible to add additional metadata (in the form of a string)
   * when an offset is committed based on the record. This can be useful (for example) to store information about which
   * node made the commit, what time the commit was made, the timestamp of the record etc.
   */
  def commitWithMetadataSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: Subscription,
      metadataFromRecord: java.util.function.Function[ConsumerRecord[K, V], String]
  ): Source[CommittableMessage[K, V], Control] =
    scaladsl.Consumer
      .commitWithMetadataSource(settings, subscription, (record: ConsumerRecord[K, V]) => metadataFromRecord(record))
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * Convenience for "at-most once delivery" semantics. The offset of each message is committed to Kafka
   * before being emitted downstream.
   */
  def atMostOnceSource[K, V](settings: ConsumerSettings[K, V],
                             subscription: Subscription): Source[ConsumerRecord[K, V], Control] =
    scaladsl.Consumer
      .atMostOnceSource(settings, subscription)
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * The `plainPartitionedSource` is a way to track automatic partition assignment from kafka.
   * When topic-partition is assigned to a consumer this source will emit tuple with assigned topic-partition and a corresponding source
   * When topic-partition is revoked then corresponding source completes
   */
  def plainPartitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription
  ): Source[Pair[TopicPartition, Source[ConsumerRecord[K, V], NotUsed]], Control] =
    scaladsl.Consumer
      .plainPartitionedSource(settings, subscription)
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * The `plainPartitionedManualOffsetSource` is similar to [[#plainPartitionedSource]] but allows the use of an offset store outside
   * of Kafka, while retaining the automatic partition assignment. When a topic-partition is assigned to a consumer, the `loadOffsetOnAssign`
   * function will be called to retrieve the offset, followed by a seek to the correct spot in the partition. The `onRevoke` function gives
   * the consumer a chance to store any uncommitted offsets, and do any other cleanup that is required.
   */
  def plainPartitionedManualOffsetSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription,
      getOffsetsOnAssign: java.util.function.Function[java.util.Set[TopicPartition], CompletionStage[
        java.util.Map[TopicPartition, Long]
      ]]
  ): Source[Pair[TopicPartition, Source[ConsumerRecord[K, V], NotUsed]], Control] =
    scaladsl.Consumer
      .plainPartitionedManualOffsetSource(
        settings,
        subscription,
        (tps: Set[TopicPartition]) =>
          getOffsetsOnAssign(tps.asJava).toScala.map(_.asScala.toMap)(ExecutionContexts.sameThreadExecutionContext),
        _ => ()
      )
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * The `plainPartitionedManualOffsetSource` is similar to [[#plainPartitionedSource]] but allows the use of an offset store outside
   * of Kafka, while retaining the automatic partition assignment. When a topic-partition is assigned to a consumer, the `loadOffsetOnAssign`
   * function will be called to retrieve the offset, followed by a seek to the correct spot in the partition. The `onRevoke` function gives
   * the consumer a chance to store any uncommitted offsets, and do any other cleanup that is required. Also allows the user access to the
   * `onPartitionsRevoked` hook, useful for cleaning up any partition-specific resources being used by the consumer.
   *
   */
  def plainPartitionedManualOffsetSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription,
      getOffsetsOnAssign: java.util.function.Function[java.util.Set[TopicPartition], CompletionStage[
        java.util.Map[TopicPartition, Long]
      ]],
      onRevoke: java.util.function.Consumer[java.util.Set[TopicPartition]]
  ): Source[Pair[TopicPartition, Source[ConsumerRecord[K, V], NotUsed]], Control] =
    scaladsl.Consumer
      .plainPartitionedManualOffsetSource(
        settings,
        subscription,
        (tps: Set[TopicPartition]) =>
          getOffsetsOnAssign(tps.asJava).toScala.map(_.asScala.toMap)(ExecutionContexts.sameThreadExecutionContext),
        (tps: Set[TopicPartition]) => onRevoke.accept(tps.asJava)
      )
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * The same as [[#plainPartitionedSource]] but with offset commit support
   */
  def committablePartitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription
  ): Source[Pair[TopicPartition, Source[CommittableMessage[K, V], NotUsed]], Control] =
    scaladsl.Consumer
      .committablePartitionedSource(settings, subscription)
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * The same as [[#plainPartitionedSource]] but with offset commit with metadata support
   */
  def commitWithMetadataPartitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription,
      metadataFromRecord: java.util.function.Function[ConsumerRecord[K, V], String]
  ): Source[Pair[TopicPartition, Source[CommittableMessage[K, V], NotUsed]], Control] =
    scaladsl.Consumer
      .commitWithMetadataPartitionedSource(settings,
                                           subscription,
                                           (record: ConsumerRecord[K, V]) => metadataFromRecord(record))
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * Special source that can use external `KafkaAsyncConsumer`. This is useful in case when
   * you have lot of manually assigned topic-partitions and want to keep only one kafka consumer
   */
  def plainExternalSource[K, V](consumer: ActorRef,
                                subscription: ManualSubscription): Source[ConsumerRecord[K, V], Control] =
    scaladsl.Consumer
      .plainExternalSource(consumer, subscription)
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * The same as [[#plainExternalSource]] but with offset commit support
   */
  def committableExternalSource[K, V](consumer: ActorRef,
                                      subscription: ManualSubscription,
                                      groupId: String,
                                      commitTimeout: FiniteDuration): Source[CommittableMessage[K, V], Control] =
    scaladsl.Consumer
      .committableExternalSource(consumer, subscription, groupId, commitTimeout)
      .mapMaterializedValue(new ConsumerControlAsJava(_))
      .asJava
      .asInstanceOf[Source[CommittableMessage[K, V], Control]]
}
