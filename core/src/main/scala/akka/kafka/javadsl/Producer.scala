/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.NotUsed
import akka.kafka.{ConsumerMessage, ProducerSettings}
import akka.kafka.ProducerMessage.{MessageOrPassThrough, _}
import akka.kafka.scaladsl
import akka.stream.javadsl.{Flow, Sink}
import org.apache.kafka.clients.producer.{ProducerRecord, Producer => KProducer}

import scala.compat.java8.FutureConverters.FutureOps

/**
 * Akka Stream connector for publishing messages to Kafka topics.
 */
object Producer {

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](settings: ProducerSettings[K, V]): Sink[ProducerRecord[K, V], CompletionStage[Done]] =
    scaladsl.Producer.plainSink(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](
    settings: ProducerSettings[K, V],
    producer: KProducer[K, V]
  ): Sink[ProducerRecord[K, V], CompletionStage[Done]] =
    scaladsl.Producer.plainSink(settings, producer)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Sink that is aware of the [[ConsumerMessage#CommittableOffset committable offset]]
   * from a [[Consumer]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * Note that there is a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def commitableSink[K, V](settings: ProducerSettings[K, V]): Sink[Message[K, V, ConsumerMessage.Committable], CompletionStage[Done]] =
    scaladsl.Producer.commitableSink(settings)
      .mapMaterializedValue(_.toJava)
      .asJava
  /**
   * Sink that is aware of the [[ConsumerMessage#CommittableOffset committable offset]]
   * from a [[Consumer]]. It will commit the consumer offset when the message has
   * been published successfully to the topic.
   *
   * Note that there is always a risk that something fails after publishing but before
   * committing, so it is "at-least once delivery" semantics.
   */
  def commitableSink[K, V](
    settings: ProducerSettings[K, V],
    producer: KProducer[K, V]
  ): Sink[Message[K, V, ConsumerMessage.Committable], CompletionStage[Done]] =
    scaladsl.Producer.commitableSink(settings, producer)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage#PartitionOffset]] from a [[Consumer]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  def transactionalSink[K, V, IN <: MessageOrPassThrough[K, V, ConsumerMessage.PartitionOffset]](
    settings: ProducerSettings[K, V],
    transactionalId: String
  ): Sink[IN, CompletionStage[Done]] =
    scaladsl.Producer.transactionalSink(settings, transactionalId)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Publish records to Kafka topics and then continue the flow.  The flow should only used with a [[Consumer]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly once transactional support.
   */
  def transactionalFlow[K, V, IN <: MessageOrPassThrough[K, V, ConsumerMessage.PartitionOffset]](settings: ProducerSettings[K, V], transactionalId: String): Flow[IN, ResultOrPassThrough[K, V, ConsumerMessage.PartitionOffset], NotUsed] =
    scaladsl.Producer.transactionalFlow(settings, transactionalId).asJava

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](settings: ProducerSettings[K, V]): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] =
    scaladsl.Producer.flow(settings)
      .asJava
      .asInstanceOf[Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed]]

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](
    settings: ProducerSettings[K, V],
    producer: KProducer[K, V]
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] =
    scaladsl.Producer.flow(settings, producer)
      .asJava
      .asInstanceOf[Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed]]
}
