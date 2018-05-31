/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
 * Classes that are used in both [[javadsl.Producer]] and
 * [[scaladsl.Producer]].
 */
object ProducerMessage {

  sealed trait MessageOrPassThrough[K, V, +PassThrough] {
    def passThrough: PassThrough
  }

  /**
   * Input element of `Producer#commitableSink` and `Producer#flow`.
   *
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   *
   * The `passThrough` field may hold any element that is passed through the `Consumer#flow`
   * and included in the [[Result]]. That is useful when some context is needed to be passed
   * on downstream operations. That could be done with unzip/zip, but this is more convenient.
   * It can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]]
   * that can be committed later in the flow.
   */
  final case class Message[K, V, +PassThrough](
      record: ProducerRecord[K, V],
      passThrough: PassThrough
  ) extends MessageOrPassThrough[K, V, PassThrough] {

    /**
     * Returns this Message if applying the predicate to
     * this Message returns true. Otherwise, returns a [[PassThroughMessage]].
     */
    def filter(p: Message[K, V, PassThrough] => Boolean): MessageOrPassThrough[K, V, PassThrough] =
      if (p(this)) this
      else PassThroughMessage(passThrough)

    /**
     * Returns this Message if applying the predicate to
     * this Message returns false. Otherwise, returns a [[PassThroughMessage]].
     */
    def filterNot(p: Message[K, V, PassThrough] => Boolean): MessageOrPassThrough[K, V, PassThrough] =
      if (!p(this)) this
      else PassThroughMessage(passThrough)
  }

  final case class PassThroughMessage[K, V, +PassThrough](
      passThrough: PassThrough
  ) extends MessageOrPassThrough[K, V, PassThrough]

  sealed trait ResultOrPassThrough[K, V, PassThrough] {
    def isEmpty: Boolean
    def nonEmpty: Boolean
  }

  /**
   * Output element of `Producer#flow`. Emitted when the message has been
   * successfully published. Includes the original message, metadata returned from KafkaProducer and the
   * `offset` of the produced message.
   */
  final case class Result[K, V, PassThrough](
      metadata: RecordMetadata,
      message: Message[K, V, PassThrough]
  ) extends ResultOrPassThrough[K, V, PassThrough] {
    val isEmpty = false
    val nonEmpty = true
    def offset: Long = metadata.offset()
  }

  final case class PassThroughResult[K, V, PassThrough](passThrough: PassThrough)
    extends ResultOrPassThrough[K, V, PassThrough] {
    val isEmpty = true
    val nonEmpty = false
  }

}
