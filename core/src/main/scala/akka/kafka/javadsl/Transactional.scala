/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.annotation.ApiMayChange
import akka.japi.Pair
import akka.kafka.ConsumerMessage.{PartitionOffset, TransactionalMessage}
import akka.kafka.ProducerMessage._
import akka.kafka._
import akka.kafka.internal.{ConsumerControlAsJava, TransactionalSourceWithContext}
import akka.kafka.javadsl.Consumer.Control
import akka.stream.javadsl.{Flow, FlowWithContext, Sink, Source, SourceWithContext}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.compat.java8.FutureConverters.FutureOps

/**
 *  Akka Stream connector to support transactions between Kafka topics.
 */
object Transactional {

  /**
   * Transactional source to setup a stream for Exactly Only Once (EoS) kafka message semantics.  To enable EoS it's
   * necessary to use the [[Transactional.sink]] or [[Transactional.flow]] (for passthrough).
   */
  def source[K, V](consumerSettings: ConsumerSettings[K, V],
                   subscription: Subscription): Source[TransactionalMessage[K, V], Control] =
    scaladsl.Transactional
      .source(consumerSettings, subscription)
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asJava

  /**
   * API MAY CHANGE
   *
   * This source is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html)
   * and [[Transactional.flowWithContext]].
   */
  @ApiMayChange
  def sourceWithContext[K, V](
      consumerSettings: ConsumerSettings[K, V],
      subscription: Subscription
  ): SourceWithContext[ConsumerRecord[K, V], PartitionOffset, Control] =
    akka.stream.scaladsl.Source
      .fromGraph(new TransactionalSourceWithContext[K, V](consumerSettings, subscription))
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asSourceWithContext(_._2)
      .map(_._1)
      .asJava

  /**
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.source]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  def sink[K, V, IN <: Envelope[K, V, ConsumerMessage.PartitionOffset]](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[IN, CompletionStage[Done]] =
    scaladsl.Transactional
      .sink(settings, transactionalId)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * API MAY CHANGE
   *
   * Sink that uses the context as [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.sourceWithContext]].
   * It will initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  @ApiMayChange
  def sinkWithContext[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[Pair[Envelope[K, V, NotUsed], PartitionOffset], CompletionStage[Done]] =
    akka.stream.scaladsl
      .Flow[Pair[Envelope[K, V, NotUsed], PartitionOffset]]
      .map(_.toScala)
      .toMat(scaladsl.Transactional.sinkWithContext(settings, transactionalId))(akka.stream.scaladsl.Keep.right)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Publish records to Kafka topics and then continue the flow.  The flow can only be used with a [[Transactional.source]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly-once transactional support.
   */
  def flow[K, V, IN <: Envelope[K, V, ConsumerMessage.PartitionOffset]](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Flow[IN, Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] =
    scaladsl.Transactional.flow(settings, transactionalId).asJava

  /**
   * API MAY CHANGE
   *
   * Publish records to Kafka topics and then continue the flow.  The flow can only be used with a [[Transactional.sourceWithContext]] that
   * carries [[ConsumerMessage.PartitionOffset]] as context.  The flow requires a unique `transactional.id` across all app
   * instances. The flow will override producer properties to enable Kafka exactly-once transactional support.
   *
   * This flow is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html)
   * and [[Transactional.sourceWithContext]].
   */
  @ApiMayChange
  def flowWithContext[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): FlowWithContext[Envelope[K, V, NotUsed],
                     ConsumerMessage.PartitionOffset,
                     Results[K, V, ConsumerMessage.PartitionOffset],
                     ConsumerMessage.PartitionOffset,
                     NotUsed] =
    scaladsl.Transactional.flowWithContext(settings, transactionalId).asJava
}
