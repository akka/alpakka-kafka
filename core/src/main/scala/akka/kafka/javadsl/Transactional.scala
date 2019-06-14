/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import org.apache.kafka.common.TopicPartition

import akka.japi.Pair
import akka.kafka.ConsumerMessage.TransactionalMessage
import akka.kafka.ProducerMessage._
import akka.kafka._
import akka.kafka.internal.ConsumerControlAsJava
import akka.kafka.javadsl.Consumer.Control
import akka.stream.javadsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}

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
   * The `partitionedSource` is a way to track automatic partition assignment from kafka.
   * Each source is setup for for Exactly Only Once (EoS) kafka message semantics.
   * To enable EoS it's necessary to use the [[Transactional.sink]] or [[Transactional.flow]] (for passthrough).
   * When Kafka rebalances partitions, all sources complete before the remaining sources are issued again.
   *
   * By generating the `transactionalId` from the [[TopicPartition]], multiple instances of your application can run
   * without having to manually assign partitions to each instance.
   */
  def partitionedSource[K, V](
      consumerSettings: ConsumerSettings[K, V],
      subscription: AutoSubscription
  ): Source[Pair[TopicPartition, Source[TransactionalMessage[K, V], NotUsed]], Control] =
    scaladsl.Transactional
      .partitionedSource(consumerSettings, subscription)
      .map {
        case (tp, source) => Pair(tp, source.asJava)
      }
      .mapMaterializedValue(ConsumerControlAsJava.apply)
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
   * Publish records to Kafka topics and then continue the flow.  The flow can only used with a [[Transactional.source]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly once transactional support.
   */
  def flow[K, V, IN <: Envelope[K, V, ConsumerMessage.PartitionOffset]](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Flow[IN, Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] =
    scaladsl.Transactional.flow(settings, transactionalId).asJava
}
