/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.collection.immutable
import scala.collection.JavaConverters._

/**
 * Classes that are used in both [[javadsl.Producer]] and
 * [[scaladsl.Producer]].
 */
object ProducerMessage {

  /**
   * Type accepted by Producer.commitableSink and `Producer.flow2` with implementations
   *
   * - [[Message]] publishes a single message to its topic, and continues in the stream as [[Result]]
   *
   * - [[MultiMessage]] publishes all messages in its `records` field, and continues in the stream as [[MultiResult]]
   *
   * - [[PassThroughMessage]] does not publish anything, and continues in the stream as [[PassThroughResult]]
   *
   * The `passThrough` field may hold any element that is passed through the `Producer.flow2`
   * and included in the [[Results]]. That is useful when some context is needed to be passed
   * on downstream operations. That could be done with unzip/zip, but this is more convenient.
   * It can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]]
   * that can be committed later in the flow.
   */
  sealed trait Messages[K, V, +PassThrough] {
    def passThrough: PassThrough
  }

  /**
   * [[Messages]] implementation that produces a single message to a Kafka topic, flows emit
   * a [[Result]] for every element processed.
   *
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   *
   * The `passThrough` field may hold any element that is passed through the `Producer.flow`
   * and included in the [[Result]]. That is useful when some context is needed to be passed
   * on downstream operations. That could be done with unzip/zip, but this is more convenient.
   * It can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]]
   * that can be committed later in the flow.
   */
  final case class Message[K, V, +PassThrough](
      record: ProducerRecord[K, V],
      passThrough: PassThrough
  ) extends Messages[K, V, PassThrough] {

    /**
     * Returns this Message if applying the predicate to
     * this Message returns true. Otherwise, returns a [[PassThroughMessage]].
     */
    def filter(p: Message[K, V, PassThrough] => Boolean): Messages[K, V, PassThrough] =
      if (p(this)) this
      else PassThroughMessage(passThrough)

    /**
     * Returns this Message if applying the predicate to
     * this Message returns false. Otherwise, returns a [[PassThroughMessage]].
     */
    def filterNot(p: Message[K, V, PassThrough] => Boolean): Messages[K, V, PassThrough] =
      if (!p(this)) this
      else PassThroughMessage(passThrough)
  }

  /**
   * [[Messages]] implementation that produces multiple message to a Kafka topics, flows emit
   *  a [[MultiResult]] for every element processed.
   *
   * Every element in `records` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   *
   * The `passThrough` field may hold any element that is passed through the `Producer.flow`
   * and included in the [[MultiResult]]. That is useful when some context is needed to be passed
   * on downstream operations. That could be done with unzip/zip, but this is more convenient.
   * It can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]]
   * that can be committed later in the flow.
   */
  final case class MultiMessage[K, V, +PassThrough](
      records: immutable.Seq[ProducerRecord[K, V]],
      passThrough: PassThrough
  ) extends Messages[K, V, PassThrough] {

    /**
     * Java API:
     * Constructor
     */
    def this(records: java.util.Collection[ProducerRecord[K, V]], passThrough: PassThrough) = {
      this(records.asScala.toList, passThrough)
    }
  }

  /**
   * [[Messages]] implementation that does not produce anything to Kafka, flows emit
   * a [[PassThroughResult]] for every element processed.
   *
   * The `passThrough` field may hold any element that is passed through the `Producer.flow`
   * and included in the [[Results]]. That is useful when some context is needed to be passed
   * on downstream operations. That could be done with unzip/zip, but this is more convenient.
   * It can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]]
   * that can be committed later in the flow.
   */
  final case class PassThroughMessage[K, V, +PassThrough](
      passThrough: PassThrough
  ) extends Messages[K, V, PassThrough]

  /**
   * Output type produced by `Producer.flow2` and `Transactional.flow`.
   */
  sealed trait Results[K, V, PassThrough] {
    def passThrough: PassThrough
  }

  /**
   * [[Results]] implementation emitted when a [[Message]] has been successfully published.
   *
   * Includes the original message, metadata returned from `KafkaProducer` and the
   * `offset` of the produced message.
   */
  final case class Result[K, V, PassThrough] private (
      metadata: RecordMetadata,
      message: Message[K, V, PassThrough]
  ) extends Results[K, V, PassThrough] {
    def offset: Long = metadata.offset()
    def passThrough: PassThrough = message.passThrough
  }

  final case class MultiResultPart[K, V] private (
      metadata: RecordMetadata,
      record: ProducerRecord[K, V]
  )

  /**
   * [[Results]] implementation emitted when all messages in a [[MultiMessage]] have been
   * successfully published.
   */
  final case class MultiResult[K, V, PassThrough] private (
      parts: immutable.Seq[MultiResultPart[K, V]],
      passThrough: PassThrough
  ) extends Results[K, V, PassThrough] {

    /**
     * Java API:
     * accessor for `parts`
     */
    def getParts(): java.util.Collection[MultiResultPart[K, V]] = parts.asJavaCollection
  }

  /**
   * [[Results]] implementation emitted when a [[PassThroughMessage]] has passed
   * through the flow.
   */
  final case class PassThroughResult[K, V, PassThrough] private (passThrough: PassThrough)
    extends Results[K, V, PassThrough]

}
