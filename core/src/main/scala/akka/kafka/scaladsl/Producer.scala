/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import akka.kafka.ProducerMessage._
import akka.kafka.internal.ProducerStage
import akka.kafka.{ConsumerMessage, ProducerSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Akka Stream connector for publishing messages to Kafka topics.
 */
object Producer {

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](settings: ProducerSettings[K, V]): Sink[ProducerRecord[K, V], Future[Done]] =
    plainSink(() => settings.createKafkaProducer(), settings.closeTimeout, settings.parallelism)

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](
    producerProvider: () => KafkaProducer[K, V],
    closeTimeout: FiniteDuration,
    parallelism: Int
  ): Sink[ProducerRecord[K, V], Future[Done]] =
    Flow[ProducerRecord[K, V]].map(record => Message(record, NotUsed))
      .via(flow(producerProvider, closeTimeout, parallelism))
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Sink that is aware of the [[ConsumerMessage#CommittableOffset committable offset]]
   * from a [[Consumer]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * Note that there is a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def commitableSink[K, V](settings: ProducerSettings[K, V]): Sink[Message[K, V, ConsumerMessage.Committable], Future[Done]] =
    commitableSink(() => settings.createKafkaProducer(), settings.closeTimeout, settings.parallelism)

  /**
   * Sink that is aware of the [[ConsumerMessage#CommittableOffset committable offset]]
   * from a [[Consumer]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * Note that there is always a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def commitableSink[K, V](
    producerProvider: () => KafkaProducer[K, V],
    closeTimeout: FiniteDuration,
    parallelism: Int
  ): Sink[Message[K, V, ConsumerMessage.Committable], Future[Done]] =
    flow[K, V, ConsumerMessage.Committable](producerProvider, closeTimeout, parallelism)
      .mapAsync(parallelism)(_.message.passThrough.commitScaladsl())
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](settings: ProducerSettings[K, V]): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    flow(() => settings.createKafkaProducer(), settings.closeTimeout, settings.parallelism)
  }

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](
    producerProvider: () => KafkaProducer[K, V],
    closeTimeout: FiniteDuration,
    parallelism: Int
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    Flow.fromGraph(new ProducerStage[K, V, PassThrough](
      closeTimeout = closeTimeout,
      producerProvider = producerProvider
    )).mapAsync(parallelism)(identity)
  }

}
