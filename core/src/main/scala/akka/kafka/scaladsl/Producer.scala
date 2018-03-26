/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.ProducerMessage._
import akka.kafka.internal.ProducerStage
import akka.kafka.{ConsumerMessage, ProducerSettings}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord, Producer => KProducer}

import scala.concurrent.Future

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
    Flow[ProducerRecord[K, V]].map(Message(_, NotUsed))
      .via(flow(settings))
      .toMat(Sink.ignore)(Keep.right)

  /**
   * The `plainSink` can be used for publishing records to Kafka topics.
   * The `record` contains a topic name to which the record is being sent, an optional
   * partition number, and an optional key and value.
   */
  def plainSink[K, V](
    settings: ProducerSettings[K, V],
    producer: KProducer[K, V]
  ): Sink[ProducerRecord[K, V], Future[Done]] =
    Flow[ProducerRecord[K, V]].map(Message(_, NotUsed))
      .via(flow(settings, producer))
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
    flow[K, V, ConsumerMessage.Committable](settings)
      .mapAsync(settings.parallelism)(_.message.passThrough.commitScaladsl())
      .toMat(Sink.ignore)(Keep.right)

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
  ): Sink[Message[K, V, ConsumerMessage.Committable], Future[Done]] =
    flow[K, V, ConsumerMessage.Committable](settings, producer)
      .mapAsync(settings.parallelism)(_.message.passThrough.commitScaladsl())
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage#PartitionOffset]] from a [[Consumer]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  def transactionalSink[K, V](
    settings: ProducerSettings[K, V],
    transactionalId: String
  ): Sink[Message[K, V, ConsumerMessage.PartitionOffset], Future[Done]] = {
    transactionalFlow(settings, transactionalId)
      .toMat(Sink.ignore)(Keep.right)
  }

  /**
   * Publish records to Kafka topics and then continue the flow.  The flow should only used with a [[Consumer]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly once transactional support.
   */
  def transactionalFlow[K, V](settings: ProducerSettings[K, V], transactionalId: String): Flow[Message[K, V, ConsumerMessage.PartitionOffset], Result[K, V, ConsumerMessage.PartitionOffset], NotUsed] = {
    require(transactionalId != null && transactionalId.length > 0, "You must define a Transactional id.")

    val txSettings = settings.withProperties(
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId,
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString
    )

    val flow = Flow.fromGraph(new ProducerStage.TransactionProducerStage[K, V, ConsumerMessage.PartitionOffset](
      txSettings.closeTimeout,
      closeProducerOnStop = true,
      () => txSettings.createKafkaProducer(),
      settings.eosCommitIntervalMs
    )).mapAsync(txSettings.parallelism)(identity)

    flowWithDispatcher(txSettings, flow)
  }

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](settings: ProducerSettings[K, V]): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    val flow = Flow.fromGraph(new ProducerStage.DefaultProducerStage[K, V, PassThrough](
      settings.closeTimeout,
      closeProducerOnStop = true,
      () => settings.createKafkaProducer()
    )).mapAsync(settings.parallelism)(identity)

    flowWithDispatcher(settings, flow)
  }

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[ConsumerMessage.CommittableOffset]] or [[ConsumerMessage.CommittableOffsetBatch]] that can
   * be committed later in the flow.
   */
  def flow[K, V, PassThrough](
    settings: ProducerSettings[K, V],
    producer: KProducer[K, V]
  ): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    val flow = Flow.fromGraph(new ProducerStage.DefaultProducerStage[K, V, PassThrough](
      closeTimeout = settings.closeTimeout,
      closeProducerOnStop = false,
      producerProvider = () => producer
    )).mapAsync(settings.parallelism)(identity)

    flowWithDispatcher(settings, flow)
  }

  private def flowWithDispatcher[PassThrough, V, K](settings: ProducerSettings[K, V], flow: Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed]) = {
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
  }
}
