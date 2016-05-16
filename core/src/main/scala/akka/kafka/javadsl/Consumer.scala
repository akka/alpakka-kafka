/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.javadsl

import akka.kafka.scaladsl
import akka.Done
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerSettings
import akka.kafka.internal.ConsumerStage.CommittableOffsetBatchImpl
import akka.kafka.internal.{CommittableConsumerStage, PlainConsumerStage}
import akka.stream.ActorAttributes
import akka.stream.javadsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.CompletionStage
import java.util.Optional
import akka.kafka.internal.WrappedConsumerControl
import akka.kafka.PartitionOffset
import akka.kafka.ClientTopicPartition
import akka.kafka.internal.WrappedCommittableMessage
import akka.kafka.internal.WrappedConsumerMessage
import akka.kafka.internal.WrappedCommittableOffsetBatch

/**
 * Akka Stream connector for subscribing to Kafka topics.
 */
object Consumer {

  /**
   * Output element of [[Consumer#atMostOnceSource atMostOnceSource]].
   */
  trait Message[K, V] {
    def key: K
    def value: V
    def partitionOffset: PartitionOffset
  }

  /**
   * Output element of [[Consumer#committableSource committableSource]].
   * The offset can be committed via the included [[CommittableOffset]].
   */
  trait CommittableMessage[K, V] extends Message[K, V] {
    def key: K
    def value: V
    def committableOffset: CommittableOffset
  }

  /**
   * Commit an offset that is included in a [[CommittableMessage]].
   * If you need to store offsets in anything other than Kafka, this API
   * should not be used.
   *
   * This interface might move into `akka.stream`
   */
  trait Committable {
    def commit(): CompletionStage[Done]
  }

  /**
   * Included in [[CommittableMessage]]. Makes it possible to
   * commit an offset with the [[Committable#commit]] method
   * or aggregate several offsets in a [[CommittableOffsetBatch batch]]
   * before committing.
   *
   * Note that the offset position that is committed to Kafka will automatically
   * be one more than the `offset` of the message, because the committed offset
   * should be the next message your application will consume,
   * i.e. lastProcessedMessageOffset + 1.
   */
  trait CommittableOffset extends Committable {
    /**
     * Information about the offset position for a
     * clientId, topic, partition.
     */
    def partitionOffset: PartitionOffset
  }

  val emptyCommittableOffsetBatch: CommittableOffsetBatch =
    new WrappedCommittableOffsetBatch(new CommittableOffsetBatchImpl(Map.empty, Map.empty))

  /**
   * For improved efficiency it is good to aggregate several [[CommittableOffset]],
   * using this class, before [[Committable#commit committing]] them. Start with
   * the [[CommittableOffsetBatch$#empty empty] batch.
   */
  trait CommittableOffsetBatch extends Committable {
    /**
     * Add/overwrite an offset position for the given clientId, topic, partition.
     */
    def updated(offset: CommittableOffset): CommittableOffsetBatch

    /**
     * Get current offset position for the given clientId, topic, partition.
     */
    def getOffset(key: ClientTopicPartition): Optional[Long]
  }

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
  def plainSource[K, V](settings: ConsumerSettings[K, V]): Source[ConsumerRecord[K, V], Control] =
    scaladsl.Consumer.plainSource(settings)
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
  def committableSource[K, V](settings: ConsumerSettings[K, V]): Source[CommittableMessage[K, V], Control] =
    scaladsl.Consumer.committableSource(settings)
      .map(new WrappedCommittableMessage(_))
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava

  /**
   * Convenience for "at-most once delivery" semantics. The offset of each message is committed to Kafka
   * before emitted downstreams.
   */
  def atMostOnceSource[K, V](settings: ConsumerSettings[K, V]): Source[Message[K, V], Control] = {
    scaladsl.Consumer.atMostOnceSource(settings)
      .map(new WrappedConsumerMessage(_))
      .mapMaterializedValue(new WrappedConsumerControl(_))
      .asJava
  }

}

