/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.annotation.ApiMayChange
import akka.kafka.ConsumerMessage.{PartitionOffset, TransactionalMessage}
import akka.kafka.ProducerMessage._
import akka.kafka.internal.{TransactionalProducerStage, TransactionalSource, TransactionalSourceWithOffsetContext}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerMessage, ConsumerSettings, ProducerSettings, Subscription}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink, Source, SourceWithContext}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.Future

/**
 * Akka Stream connector to support transactions between Kafka topics.
 */
object Transactional {

  /**
   * Transactional source to setup a stream for Exactly Only Once (EoS) kafka message semantics.  To enable EoS it's
   * necessary to use the [[Transactional.sink]] or [[Transactional.flow]] (for passthrough).
   */
  def source[K, V](settings: ConsumerSettings[K, V],
                   subscription: Subscription): Source[TransactionalMessage[K, V], Control] =
    Source.fromGraph(new TransactionalSource[K, V](settings, subscription))

  /**
   * API MAY CHANGE
   *
   * This source is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html)
   * and [[Transactional.flowWithOffsetContext]].
   */
  @ApiMayChange
  def sourceWithOffsetContext[K, V](
      settings: ConsumerSettings[K, V],
      subscription: Subscription
  ): SourceWithContext[ConsumerRecord[K, V], PartitionOffset, Control] =
    Source
      .fromGraph(new TransactionalSourceWithOffsetContext[K, V](settings, subscription))
      .asSourceWithContext(_._2)
      .map(_._1)

  /**
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.source]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  def sink[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[Envelope[K, V, ConsumerMessage.PartitionOffset], Future[Done]] =
    flow(settings, transactionalId).toMat(Sink.ignore)(Keep.right)

  /**
   * API MAY CHANGE
   *
   * Sink that requires the context to be [[ConsumerMessage.PartitionOffset]] from a [[Transactional.sourceWithOffsetContext]].
   * It will initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  @ApiMayChange
  def sinkWithOffsetContext[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[(Envelope[K, V, NotUsed], PartitionOffset), Future[Done]] =
    Flow[(Envelope[K, V, NotUsed], PartitionOffset)]
      .map {
        case (env, offset) =>
          env.withPassThrough(offset)
      }
      .toMat(sink(settings, transactionalId))(Keep.right)

  /**
   * Publish records to Kafka topics and then continue the flow. The flow can only be used with a [[Transactional.source]] that
   * emits a [[ConsumerMessage.TransactionalMessage]]. The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly-once transactional support.
   */
  def flow[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Flow[Envelope[K, V, ConsumerMessage.PartitionOffset], Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] = {
    require(transactionalId != null && transactionalId.length > 0, "You must define a Transactional id.")
    require(settings.producerFactorySync.isEmpty, "You cannot use a shared or external producer factory.")

    val txSettings = settings.withProperties(
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId,
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString
    )

    val flow = Flow
      .fromGraph(
        new TransactionalProducerStage[K, V, ConsumerMessage.PartitionOffset](txSettings)
      )
      .mapAsync(txSettings.parallelism)(identity)

    flowWithDispatcher(txSettings, flow)
  }

  /**
   * API MAY CHANGE
   *
   * Publish records to Kafka topics and then continue the flow. The flow can only be used with a [[Transactional.sourceWithOffsetContext]] which
   * carries [[ConsumerMessage.PartitionOffset]] as context. The flow requires a unique `transactional.id` across all app
   * instances. The flow will override producer properties to enable Kafka exactly-once transactional support.
   *
   * This flow is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html)
   * and [[Transactional.sourceWithOffsetContext]].
   */
  @ApiMayChange
  def flowWithOffsetContext[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): FlowWithContext[Envelope[K, V, NotUsed],
                     ConsumerMessage.PartitionOffset,
                     Results[K, V, ConsumerMessage.PartitionOffset],
                     ConsumerMessage.PartitionOffset,
                     NotUsed] = {
    val noContext: Flow[Envelope[K, V, PartitionOffset], Results[K, V, PartitionOffset], NotUsed] =
      flow(settings, transactionalId)
    noContext
      .asFlowWithContext[Envelope[K, V, NotUsed], PartitionOffset, PartitionOffset]({
        case (env, c) => env.withPassThrough(c)
      })(res => res.passThrough)
  }

  private def flowWithDispatcher[PassThrough, V, K](
      settings: ProducerSettings[K, V],
      flow: Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]
  ) =
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
}
