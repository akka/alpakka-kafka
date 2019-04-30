/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.NotUsed
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.collection.immutable
import scala.collection.JavaConverters._

/**
 * Classes that are used in both [[javadsl.Producer]] and
 * [[scaladsl.Producer]].
 */
object ProducerMessage {

  /**
   * Type accepted by `Producer.committableSink` and `Producer.flexiFlow` with implementations
   *
   * - [[Message]] publishes a single message to its topic, and continues in the stream as [[Result]]
   *
   * - [[MultiMessage]] publishes all messages in its `records` field, and continues in the stream as [[MultiResult]]
   *
   * - [[PassThroughMessage]] does not publish anything, and continues in the stream as [[PassThroughResult]]
   *
   * The `passThrough` field may hold any element that is passed through the `Producer.flexiFlow`
   * and included in the [[Results]]. That is useful when some context is needed to be passed
   * on downstream operations. That could be done with unzip/zip, but this is more convenient.
   * It can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]]
   * that can be committed later in the flow.
   */
  sealed trait Envelope[K, V, +PassThrough] {
    def passThrough: PassThrough
    def withPassThrough[PassThrough2](value: PassThrough2): Envelope[K, V, PassThrough2]
  }

  /**
   * [[Envelope]] implementation that produces a single message to a Kafka topic, flows emit
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
  ) extends Envelope[K, V, PassThrough] {
    override def withPassThrough[PassThrough2](value: PassThrough2): Message[K, V, PassThrough2] =
      copy(passThrough = value)
  }

  /**
   * Create a message containing the `record` and a `passThrough`.
   *
   * @tparam K the type of keys
   * @tparam V the type of values
   * @tparam PassThrough the type of data passed through
   */
  def single[K, V, PassThrough](
      record: ProducerRecord[K, V],
      passThrough: PassThrough
  ): Envelope[K, V, PassThrough] = Message(record, passThrough)

  /**
   * Create a message containing the `record`.
   *
   * @tparam K the type of keys
   * @tparam V the type of values
   */
  def single[K, V](record: ProducerRecord[K, V]): Envelope[K, V, NotUsed] = Message(record, NotUsed)

  /**
   * [[Envelope]] implementation that produces multiple message to a Kafka topics, flows emit
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
  ) extends Envelope[K, V, PassThrough] {

    /**
     * Java API:
     * Constructor
     */
    def this(records: java.util.Collection[ProducerRecord[K, V]], passThrough: PassThrough) = {
      this(records.asScala.toList, passThrough)
    }

    override def withPassThrough[PassThrough2](value: PassThrough2): Envelope[K, V, PassThrough2] =
      copy(passThrough = value)
  }

  /**
   * Create a multi-message containing several `records` and one `passThrough`.
   *
   * @tparam K the type of keys
   * @tparam V the type of values
   * @tparam PassThrough the type of data passed through
   */
  def multi[K, V, PassThrough](
      records: immutable.Seq[ProducerRecord[K, V]],
      passThrough: PassThrough
  ): Envelope[K, V, PassThrough] = MultiMessage(records, passThrough)

  /**
   * Create a multi-message containing several `records`.
   *
   * @tparam K the type of keys
   * @tparam V the type of values
   */
  def multi[K, V](
      records: immutable.Seq[ProducerRecord[K, V]]
  ): Envelope[K, V, NotUsed] = MultiMessage(records, NotUsed)

  /**
   * Java API:
   * Create a multi-message containing several `records` and one `passThrough`.
   *
   * @tparam K the type of keys
   * @tparam V the type of values
   * @tparam PassThrough the type of data passed through
   */
  def multi[K, V, PassThrough](
      records: java.util.Collection[ProducerRecord[K, V]],
      passThrough: PassThrough
  ): Envelope[K, V, PassThrough] = new MultiMessage(records, passThrough)

  /**
   * Java API:
   * Create a multi-message containing several `records`.
   *
   * @tparam K the type of keys
   * @tparam V the type of values
   */
  def multi[K, V](
      records: java.util.Collection[ProducerRecord[K, V]]
  ): Envelope[K, V, NotUsed] = new MultiMessage(records, NotUsed)

  /**
   * [[Envelope]] implementation that does not produce anything to Kafka, flows emit
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
  ) extends Envelope[K, V, PassThrough] {
    override def withPassThrough[PassThrough2](value: PassThrough2): Envelope[K, V, PassThrough2] =
      copy(passThrough = value)
  }

  /**
   * Create a pass-through message not containing any records.
   * In some cases the type parameters need to be specified explicitly.
   *
   * @tparam K the type of keys
   * @tparam V the type of values
   * @tparam PassThrough the type of data passed through
   */
  def passThrough[K, V, PassThrough](passThrough: PassThrough): Envelope[K, V, PassThrough] =
    PassThroughMessage(passThrough)

  /**
   * Output type produced by `Producer.flexiFlow` and `Transactional.flow`.
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
