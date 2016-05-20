/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka

import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Classes that are used in both [[javadsl.Producer]] and
 * [[scaladsl.Producer]].
 */
object ProducerMessage {

  /**
   * Input element of `Consumer#commitableSink` and `Consumer#flow`.
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
  )

  /**
   * Output element of `Consumer#flow`. Emitted when the message has been
   * successfully published. Includes the original message and the
   * `offset` of the produced message.
   */
  final case class Result[K, V, PassThrough](
    offset: Long,
    message: Message[K, V, PassThrough]
  )

}

