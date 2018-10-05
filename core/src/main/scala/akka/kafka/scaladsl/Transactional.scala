/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.ConsumerMessage.TransactionalMessage
import akka.kafka.ProducerMessage._
import akka.kafka.internal.ProducerStage
import akka.kafka.internal.TransactionalSource
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerMessage, ConsumerSettings, ProducerSettings, Subscription}
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
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
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.source]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  def sink[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[Envelope[K, V, ConsumerMessage.PartitionOffset], Future[Done]] =
    flow(settings, transactionalId).toMat(Sink.ignore)(Keep.right)

  /**
   * Publish records to Kafka topics and then continue the flow.  The flow should only used with a [[Transactional.source]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly once transactional support.
   */
  def flow[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Flow[Envelope[K, V, ConsumerMessage.PartitionOffset], Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] = {
    require(transactionalId != null && transactionalId.length > 0, "You must define a Transactional id.")

    val txSettings = settings.withProperties(
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId,
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString
    )

    val flow = Flow
      .fromGraph(
        new ProducerStage.TransactionProducerStage[K, V, ConsumerMessage.PartitionOffset](
          txSettings.closeTimeout,
          closeProducerOnStop = true,
          () => txSettings.createKafkaProducer(),
          settings.eosCommitInterval
        )
      )
      .mapAsync(txSettings.parallelism)(identity)

    flowWithDispatcher(txSettings, flow)
  }

  private def flowWithDispatcher[PassThrough, V, K](
      settings: ProducerSettings[K, V],
      flow: Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]
  ) =
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
}
