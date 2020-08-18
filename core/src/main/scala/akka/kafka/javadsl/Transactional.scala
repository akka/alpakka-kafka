/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.annotation.ApiMayChange
import akka.japi.Pair
import akka.kafka.ConsumerMessage.{PartitionOffset, TransactionalMessage}
import akka.kafka.ProducerMessage._
import akka.kafka._
import akka.kafka.internal.{ConsumerControlAsJava, TransactionalSourceWithOffsetContext}
import akka.kafka.javadsl.Consumer.Control
import akka.stream.javadsl._
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
   * and [[Transactional.flowWithOffsetContext]].
   */
  @ApiMayChange
  def sourceWithOffsetContext[K, V](
      consumerSettings: ConsumerSettings[K, V],
      subscription: Subscription
  ): SourceWithContext[ConsumerRecord[K, V], PartitionOffset, Control] =
    akka.stream.scaladsl.Source
      .fromGraph(new TransactionalSourceWithOffsetContext[K, V](consumerSettings, subscription))
      .mapMaterializedValue(ConsumerControlAsJava.apply)
      .asSourceWithContext(_._2)
      .map(_._1)
      .asJava

//  /**
//   * Internal API. Work in progress.
//   *
//   * The `partitionedSource` is a way to track automatic partition assignment from kafka.
//   * Each source is setup for for Exactly Only Once (EoS) kafka message semantics.
//   * To enable EoS it's necessary to use the [[Transactional.sink]] or [[Transactional.flow]] (for passthrough).
//   * When Kafka rebalances partitions, all sources complete before the remaining sources are issued again.
//   *
//   * By generating the `transactionalId` from the [[TopicPartition]], multiple instances of your application can run
//   * without having to manually assign partitions to each instance.
//   */
//  @ApiMayChange
//  @InternalApi
//  private[kafka] def partitionedSource[K, V](
//      consumerSettings: ConsumerSettings[K, V],
//      subscription: AutoSubscription
//  ): Source[Pair[TopicPartition, Source[TransactionalMessage[K, V], NotUsed]], Control] =
//    scaladsl.Transactional
//      .partitionedSource(consumerSettings, subscription)
//      .map {
//        case (tp, source) => Pair(tp, source.asJava)
//      }
//      .mapMaterializedValue(ConsumerControlAsJava.apply)
//      .asJava

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
   * Sink that uses the context as [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.sourceWithOffsetContext]].
   * It will initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  @ApiMayChange
  def sinkWithOffsetContext[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[Pair[Envelope[K, V, NotUsed], PartitionOffset], CompletionStage[Done]] =
    akka.stream.scaladsl
      .Flow[Pair[Envelope[K, V, NotUsed], PartitionOffset]]
      .map(_.toScala)
      .toMat(scaladsl.Transactional.sinkWithOffsetContext(settings, transactionalId))(akka.stream.scaladsl.Keep.right)
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
   * Publish records to Kafka topics and then continue the flow.  The flow can only be used with a [[Transactional.sourceWithOffsetContext]] that
   * carries [[ConsumerMessage.PartitionOffset]] as context.  The flow requires a unique `transactional.id` across all app
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
                     NotUsed] =
    scaladsl.Transactional.flowWithOffsetContext(settings, transactionalId).asJava
}
