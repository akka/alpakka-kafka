/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.ActorRef
import akka.japi.Pair
import akka.kafka.ConsumerMessage.{CommittableMessage, Message}
import akka.kafka.internal.ConsumerStage.WrappedConsumerControl
import akka.kafka.{AutoSubscription, ConsumerSettings, ManualSubscription, Subscription, scaladsl}
import akka.stream.javadsl.Source
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

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
     * Shutdown status. The `CompletionStage` will be completed when the stage has been shut down
     * and the underlying `KafkaConsumer` has been closed. Shutdown can be triggered
     * from downstream cancellation, errors, or [[#shutdown]].
     */
    def isShutdown: CompletionStage[Done]
  }

  /**
   * The `plainSource` emits `ConsumerRecord` elements (as received from the underlying `KafkaConsumer`).
   * It has not support for committing offsets to Kafka. It can be used when offset is stored externally
   * or with auto-commit (note that auto-commit is by default disabled).
   *
   * The consumer application need not use Kafka's built-in offset storage, it can store offsets in a store of its own
   * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
   * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
   * possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
   * stronger than the "at-least once" semantics you get with Kafka's offset commit functionality.
   */
  def plainSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[ConsumerRecord[K, V], Control] =
    scaladsl.Consumer.plainSource(settings, subscription)
      .mapMaterializedValue(new WrappedConsumerControl(_))
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
  def committableSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[CommittableMessage[K, V], Control] =
    scaladsl.Consumer.committableSource(settings, subscription)
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava

  /**
   * Convenience for "at-most once delivery" semantics. The offset of each message is committed to Kafka
   * before emitted downstreams.
   */
  def atMostOnceSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[Message[K, V], Control] =
    scaladsl.Consumer.atMostOnceSource(settings, subscription)
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava

  /**
   * The `plainPartitionedSource` is a way to track automatic partition assignment from kafka.
   * When topic-partition is assigned to a consumer this source will emit tuple with assigned topic-partition and a corresponding source
   * When topic-partition is revoked then corresponding source completes
   */
  def plainPartitionedSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription): Source[Pair[TopicPartition, Source[ConsumerRecord[K, V], NotUsed]], Control] = {
    scaladsl.Consumer.plainPartitionedSource(settings, subscription)
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava
  }

  /**
   * The same as [[#plainPartitionedSource]] but with offset commit support
   */
  def committablePartitionedSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription): Source[Pair[TopicPartition, Source[CommittableMessage[K, V], NotUsed]], Control] = {
    scaladsl.Consumer.committablePartitionedSource(settings, subscription)
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava
  }

  /**
   * Special source that can use external `KafkaAsyncConsumer`. This is useful in case when
   * you have lot of manually assigned topic-partitions and want to keep only one kafka consumer
   */
  def plainExternalSource[K, V](consumer: ActorRef, subscription: ManualSubscription): Source[ConsumerRecord[K, V], Control] = {
    scaladsl.Consumer.plainExternalSource(consumer, subscription)
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava
  }

  /**
   * The same as [[#plainExternalSource]] but with offset commit support
   */
  def committableExternalSource[K, V](consumer: ActorRef, subscription: ManualSubscription, clientId: String, commitTimeout: FiniteDuration): Source[CommittableMessage[K, V], Control] = {
    scaladsl.Consumer.committableExternalSource(consumer, subscription, clientId, commitTimeout)
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava
  }
}

