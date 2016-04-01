/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import scala.concurrent.Future
import akka.NotUsed
import akka.kafka.ProducerSettings
import akka.kafka.internal.ProducerStage
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.ProducerRecord
import akka.stream.ActorAttributes

/**
 * Akka Stream connector for publishing messages to Kafka topics.
 */
object Producer {

  /**
   * Input element of [[#commitableSink]] and [[#flow]].
   *
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   *
   * The `passThrough` field may hold any element that is passed through the [[#flow]]
   * and included in the [[Result]]. That is useful when some context is needed to be passed
   * on downstream operations. That could be done with unzip/zip, but this is more convenient.
   * It can for example be a [[Consumer.CommittableOffset]] or [[Consumer.CommittableOffsetBatch]]
   * that can be committed later in the flow.
   */
  final case class Message[K, V, +PassThrough](
    record: ProducerRecord[K, V],
    passThrough: PassThrough
  )

  /**
   * Output element of [[#flow]]. Emitted when the message has been
   * successfully published. Includes the original message and the
   * `offset` of the produced message.
   */
  final case class Result[K, V, PassThrough](
    offset: Long,
    message: Message[K, V, PassThrough]
  )

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](settings: ProducerSettings[K, V]): Sink[ProducerRecord[K, V], NotUsed] =
    Flow[ProducerRecord[K, V]].map(record => Message(record, NotUsed))
      .viaMat(flow(settings))(Keep.right)
      .to(Sink.ignore)

  /**
   * Sink that is aware of the [[Consumer#CommittableOffset committable offset]]
   * from a [[Consumer]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * Note that there is always a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def commitableSink[K, V](settings: ProducerSettings[K, V]): Sink[Message[K, V, Consumer.Committable], NotUsed] =
    flow[K, V, Consumer.Committable](settings)
      .mapAsync(settings.parallelism)(_.message.passThrough.commit())
      .to(Sink.ignore)

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[Consumer.CommittableOffset]] or [[Consumer.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](settings: ProducerSettings[K, V]): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    val flow = Flow.fromGraph(new ProducerStage[K, V, PassThrough](
      settings,
      () => settings.createKafkaProducer()
    ))
      .mapAsync(settings.parallelism)(identity)
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
  }

}

