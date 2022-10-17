/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage
import akka.annotation.ApiMayChange
import akka.kafka.ConsumerMessage.Committable
import akka.kafka.ProducerMessage._
import akka.kafka.{scaladsl, CommitterSettings, ConsumerMessage, ProducerSettings}
import akka.stream.javadsl.{Flow, FlowWithContext, Sink}
import akka.{japi, Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.annotation.nowarn
import scala.compat.java8.FutureConverters._

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
   *
   * @deprecated Pass in external or shared producer using `ProducerSettings.withProducerFactory` or `ProducerSettings.withProducer`, since 2.0.0
   */
  @Deprecated
  def plainSink[K, V](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Sink[ProducerRecord[K, V], CompletionStage[Done]] =
    plainSink(settings.withProducer(producer))

  /**
   * Create a sink that is aware of the [[ConsumerMessage.Committable committable offset]]
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
   * @deprecated use `committableSink(ProducerSettings, CommitterSettings)` instead, since 2.0.0
   */
  @Deprecated
  def committableSink[K, V, IN <: Envelope[K, V, ConsumerMessage.Committable]](
      settings: ProducerSettings[K, V]
  ): Sink[IN, CompletionStage[Done]] = {
    @nowarn("cat=deprecation")
    val sink: Sink[IN, CompletionStage[Done]] = scaladsl.Producer
      .committableSink(settings)
      .mapMaterializedValue(_.toJava)
      .asJava
    sink
  }

  /**
   * Create a sink that is aware of the [[ConsumerMessage.Committable committable offset]]
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
   * @deprecated use `committableSink(ProducerSettings, CommitterSettings)` instead, since 2.0.0
   */
  @Deprecated
  def committableSink[K, V](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Sink[Envelope[K, V, ConsumerMessage.Committable], CompletionStage[Done]] =
    committableSink(settings.withProducer(producer))

  /**
   * Create a sink that is aware of the [[ConsumerMessage.Committable committable offset]]
   * from a [[Consumer.committableSource]]. The offsets are batched and committed regularly.
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
      producerSettings: ProducerSettings[K, V],
      committerSettings: CommitterSettings
  ): Sink[IN, CompletionStage[Done]] =
    scaladsl.Producer
      .committableSink(producerSettings, committerSettings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Create a sink that is aware of the [[ConsumerMessage.Committable committable offset]] passed as
   * context from a [[Consumer.sourceWithOffsetContext]]. The offsets are batched and committed regularly.
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
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/880")
  def committableSinkWithOffsetContext[K, V, IN <: Envelope[K, V, _], C <: Committable](
      producerSettings: ProducerSettings[K, V],
      committerSettings: CommitterSettings
  ): Sink[akka.japi.Pair[IN, C], CompletionStage[Done]] =
    committableSink(producerSettings, committerSettings)
      .contramap(new akka.japi.function.Function[japi.Pair[IN, C], Envelope[K, V, C]] {
        override def apply(p: japi.Pair[IN, C]) = p.first.withPassThrough(p.second)
      })

  /**
   * Create a flow to publish records to Kafka topics and then pass it on.
   *
   * The records must be wrapped in a [[akka.kafka.ProducerMessage.Message Message]] and continue in the stream as [[akka.kafka.ProducerMessage.Result Result]].
   *
   * The messages support the possibility to pass through arbitrary data, which can for example be a [[ConsumerMessage.CommittableOffset CommittableOffset]]
   * or [[ConsumerMessage.CommittableOffsetBatch CommittableOffsetBatch]] that can
   * be committed later in the flow.
   *
   * @deprecated use `flexiFlow` instead, since 0.21
   */
  @Deprecated
  def flow[K, V, PassThrough](
      settings: ProducerSettings[K, V]
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    @nowarn("cat=deprecation")
    val flow = scaladsl.Producer
      .flow(settings)
      .asJava
      .asInstanceOf[Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed]]
    flow
  }

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
   * API MAY CHANGE
   *
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
   *
   * @tparam C the flow context type
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/880")
  def flowWithContext[K, V, C](
      settings: ProducerSettings[K, V]
  ): FlowWithContext[Envelope[K, V, NotUsed], C, Results[K, V, C], C, NotUsed] =
    scaladsl.Producer.flowWithContext(settings).asJava

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
   *
   * @deprecated use `flexiFlow` instead, since 0.21
   */
  @Deprecated
  def flow[K, V, PassThrough](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] =
    flow(settings.withProducer(producer))

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
   *
   * @deprecated Pass in external or shared producer using `ProducerSettings.withProducerFactory` or `ProducerSettings.withProducer`, since 2.0.0
   */
  @Deprecated
  def flexiFlow[K, V, PassThrough](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed] =
    flexiFlow(settings.withProducer(producer))

  /**
   * API MAY CHANGE
   *
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
   *
   * Supports sharing a Kafka Producer instance.
   *
   * @tparam C the flow context type
   *
   * @deprecated Pass in external or shared producer using `ProducerSettings.withProducerFactory` or `ProducerSettings.withProducer`, since 2.0.0
   */
  @Deprecated
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/880")
  def flowWithContext[K, V, C](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): FlowWithContext[Envelope[K, V, NotUsed], C, Results[K, V, C], C, NotUsed] =
    flowWithContext(settings.withProducer(producer))

}
