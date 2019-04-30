/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.annotation.ApiMayChange
import akka.japi.Pair
import akka.{Done, NotUsed}
import akka.kafka.ProducerMessage._
import akka.kafka.{scaladsl, ConsumerMessage, ProducerSettings}
import akka.stream.javadsl.{Flow, Sink}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.compat.java8.FutureConverters.FutureOps

/**
 * Akka Stream connector for publishing messages to Kafka topics.
 */
object Producer {

  /**
   * Create a sink for publishing records to Kafka topics.
   *
   * The [[org.apache.kafka.clients.producer.ProducerRecord Kafka ProducerRecord]] contains the topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](settings: ProducerSettings[K, V]): Sink[ProducerRecord[K, V], CompletionStage[Done]] =
    scaladsl.Producer
      .plainSink(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a sink for publishing records to Kafka topics.
   *
   * The [[org.apache.kafka.clients.producer.ProducerRecord Kafka ProducerRecord]] contains the topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   *
   * Supports sharing a Kafka Producer instance.
   */
  def plainSink[K, V](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Sink[ProducerRecord[K, V], CompletionStage[Done]] =
    scaladsl.Producer
      .plainSink(settings, producer)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a sink that is aware of the [[ConsumerMessage.CommittableOffset committable offset]]
   * from a [[Consumer.committableSource]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, but commits the offset
   *
   * Note that there is a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def committableSink[K, V, IN <: Envelope[K, V, ConsumerMessage.Committable]](
      settings: ProducerSettings[K, V]
  ): Sink[IN, CompletionStage[Done]] =
    scaladsl.Producer
      .committableSink(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a sink that is aware of the [[ConsumerMessage.CommittableOffset committable offset]]
   * from a [[Consumer.committableSource]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, but commits the offset
   *
   * Note that there is a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   *
   * @deprecated use `committableSink` instead, since 1.0-RC1
   */
  @Deprecated
  def commitableSink[K, V, IN <: Envelope[K, V, ConsumerMessage.Committable]](
      settings: ProducerSettings[K, V]
  ): Sink[IN, CompletionStage[Done]] = committableSink(settings)

  /**
   * Create a sink that is aware of the [[ConsumerMessage.CommittableOffset committable offset]]
   * from a [[Consumer.committableSource]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, but commits the offset
   *
   *
   * Note that there is always a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   *
   * Supports sharing a Kafka Producer instance.
   */
  def committableSink[K, V](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Sink[Envelope[K, V, ConsumerMessage.Committable], CompletionStage[Done]] =
    scaladsl.Producer
      .committableSink(settings, producer)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a sink that is aware of the [[ConsumerMessage.CommittableOffset committable offset]]
   * from a [[Consumer.committableSource]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and commits the offset
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, but commits the offset
   *
   *
   * Note that there is always a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   *
   * Supports sharing a Kafka Producer instance.
   *
   * @deprecated use `committableSink` instead, since 1.0-RC1
   */
  @Deprecated
  def commitableSink[K, V](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Sink[Envelope[K, V, ConsumerMessage.Committable], CompletionStage[Done]] = committableSink(settings, producer)

  /**
   * Create a flow to publish records to Kafka topics and then pass it on.
   *
   * The records must be wrapped in a [[akka.kafka.ProducerMessage.Message Message]] and continue in the stream as [[akka.kafka.ProducerMessage.Result Result]].
   *
   * The messages support the possibility to pass through arbitrary data, which can for example be a [[ConsumerMessage.CommittableOffset CommittableOffset]]
   * or [[ConsumerMessage.CommittableOffsetBatch CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  @deprecated("prefer flexiFlow over this flow implementation", "0.21")
  def flow[K, V, PassThrough](
      settings: ProducerSettings[K, V]
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] =
    scaladsl.Producer
      .flow(settings)
      .asJava
      .asInstanceOf[Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed]]

  /**
   * Create a flow to conditionally publish records to Kafka topics and then pass it on.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and continues in the stream as [[akka.kafka.ProducerMessage.Result Result]]
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and continues in the stream as [[akka.kafka.ProducerMessage.MultiResult MultiResult]]
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, and continues in the stream as [[akka.kafka.ProducerMessage.PassThroughResult PassThroughResult]]
   *
   * The messages support the possibility to pass through arbitrary data, which can for example be a [[ConsumerMessage.CommittableOffset CommittableOffset]]
   * or [[ConsumerMessage.CommittableOffsetBatch CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flexiFlow[K, V, PassThrough](
      settings: ProducerSettings[K, V]
  ): Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed] =
    scaladsl.Producer
      .flexiFlow(settings)
      .asJava
      .asInstanceOf[Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]]

  /**
   * Create a flow to conditionally publish records to Kafka topics and then pass it on.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and continues in the stream as [[akka.kafka.ProducerMessage.Result Result]]
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and continues in the stream as [[akka.kafka.ProducerMessage.MultiResult MultiResult]]
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, and continues in the stream as [[akka.kafka.ProducerMessage.PassThroughResult PassThroughResult]]
   *
   * This flow is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html).
   */
  @ApiMayChange
  def withContext[K, V, C](
      settings: ProducerSettings[K, V]
  ): Flow[Pair[Envelope[K, V, NotUsed], C], Pair[Results[K, V, C], C], NotUsed] =
    akka.stream.scaladsl
      .Flow[Pair[Envelope[K, V, NotUsed], C]]
      .map(pair => pair.first.withPassThrough(pair.second))
      .via(scaladsl.Producer.flexiFlow(settings))
      .map(res => Pair(res, res.passThrough))
      .asJava

  /**
   * Create a flow to publish records to Kafka topics and then pass it on.
   *
   * The records must be wrapped in a [[akka.kafka.ProducerMessage.Message Message]] and continue in the stream as [[akka.kafka.ProducerMessage.Result Result]].
   *
   * The messages support the possibility to pass through arbitrary data, which can for example be a [[ConsumerMessage.CommittableOffset CommittableOffset]]
   * or [[ConsumerMessage.CommittableOffsetBatch CommittableOffsetBatch]] that can
   * be committed later in the flow.
   *
   * Supports sharing a Kafka Producer instance.
   */
  @deprecated("prefer flexiFlow over this flow implementation", "0.21")
  def flow[K, V, PassThrough](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] =
    scaladsl.Producer
      .flow(settings, producer)
      .asJava
      .asInstanceOf[Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed]]

  /**
   * Create a flow to conditionally publish records to Kafka topics and then pass it on.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and continues in the stream as [[akka.kafka.ProducerMessage.Result Result]]
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and continues in the stream as [[akka.kafka.ProducerMessage.MultiResult MultiResult]]
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, and continues in the stream as [[akka.kafka.ProducerMessage.PassThroughResult PassThroughResult]]
   *
   * The messages support the possibility to pass through arbitrary data, which can for example be a [[ConsumerMessage.CommittableOffset CommittableOffset]]
   * or [[ConsumerMessage.CommittableOffsetBatch CommittableOffsetBatch]] that can
   * be committed later in the flow.
   *
   * Supports sharing a Kafka Producer instance.
   */
  def flexiFlow[K, V, PassThrough](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed] =
    scaladsl.Producer
      .flexiFlow(settings, producer)
      .asJava
      .asInstanceOf[Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]]
}
