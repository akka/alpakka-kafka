/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.annotation.ApiMayChange
import akka.kafka.ConsumerMessage.Committable
import akka.kafka.ProducerMessage._
import akka.kafka.internal.DefaultProducerStage
import akka.kafka.{CommitterSettings, ConsumerMessage, ProducerSettings}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future

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
  def plainSink[K, V](settings: ProducerSettings[K, V]): Sink[ProducerRecord[K, V], Future[Done]] =
    Flow[ProducerRecord[K, V]]
      .map(Message(_, NotUsed))
      .via(flexiFlow(settings))
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink for publishing records to Kafka topics.
   *
   * The [[org.apache.kafka.clients.producer.ProducerRecord Kafka ProducerRecord]] contains the topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   *
   * Supports sharing a Kafka Producer instance.
   */
  @deprecated(
    "Pass in external or shared producer using ProducerSettings.withProducerFactory or ProducerSettings.withProducer",
    "1.1.1"
  )
  def plainSink[K, V](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Sink[ProducerRecord[K, V], Future[Done]] =
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
   */
  @deprecated("use `committableSink(ProducerSettings, CommitterSettings)` instead", "1.1.1")
  def committableSink[K, V](
      settings: ProducerSettings[K, V]
  ): Sink[Envelope[K, V, ConsumerMessage.Committable], Future[Done]] =
    flexiFlow[K, V, ConsumerMessage.Committable](settings)
      .mapAsync(settings.parallelism)(_.passThrough.commitInternal())
      .toMat(Sink.ignore)(Keep.right)

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
   */
  @deprecated("use `committableSink(ProducerSettings, CommitterSettings)` instead", "1.1.1")
  def committableSink[K, V](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): Sink[Envelope[K, V, ConsumerMessage.Committable], Future[Done]] =
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
  def committableSink[K, V](
      producerSettings: ProducerSettings[K, V],
      committerSettings: CommitterSettings
  ): Sink[Envelope[K, V, ConsumerMessage.Committable], Future[Done]] =
    flexiFlow[K, V, ConsumerMessage.Committable](producerSettings)
      .map(_.passThrough)
      .toMat(Committer.sink(committerSettings))(Keep.right)

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
  def committableSinkWithOffsetContext[K, V](
      producerSettings: ProducerSettings[K, V],
      committerSettings: CommitterSettings
  ): Sink[(Envelope[K, V, _], Committable), Future[Done]] =
    committableSink(producerSettings, committerSettings)
      .contramap {
        case (env, offset) =>
          env.withPassThrough(offset)
      }

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
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    val flow = Flow
      .fromGraph(
        new DefaultProducerStage[K, V, PassThrough, Message[K, V, PassThrough], Result[K, V, PassThrough]](
          settings
        )
      )
      .mapAsync(settings.parallelism)(identity)

    flowWithDispatcher(settings, flow)
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
  ): Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed] = {
    val flow = Flow
      .fromGraph(
        new DefaultProducerStage[K, V, PassThrough, Envelope[K, V, PassThrough], Results[K, V, PassThrough]](
          settings
        )
      )
      .mapAsync(settings.parallelism)(identity)

    flowWithDispatcherEnvelope(settings, flow)
  }

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
    flexiFlow[K, V, C](settings)
      .asFlowWithContext[Envelope[K, V, NotUsed], C, C]({
        case (env, c) => env.withPassThrough(c)
      })(res => res.passThrough)

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
   */
  @deprecated(
    "Pass in external or shared producer using ProducerSettings.withProducerFactory or ProducerSettings.withProducer",
    "1.1.1"
  )
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
   */
  @deprecated(
    "Pass in external or shared producer using ProducerSettings.withProducerFactory or ProducerSettings.withProducer",
    "1.1.1"
  )
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/880")
  def flowWithContext[K, V, C](
      settings: ProducerSettings[K, V],
      producer: org.apache.kafka.clients.producer.Producer[K, V]
  ): FlowWithContext[Envelope[K, V, NotUsed], C, Results[K, V, C], C, NotUsed] =
    flowWithContext(settings.withProducer(producer))

  private def flowWithDispatcher[PassThrough, V, K](
      settings: ProducerSettings[K, V],
      flow: Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed]
  ) =
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))

  private def flowWithDispatcherEnvelope[PassThrough, V, K](
      settings: ProducerSettings[K, V],
      flow: Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]
  ) =
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
}
