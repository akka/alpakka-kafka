/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.kafka.ProducerMessage._
import akka.kafka.{scaladsl, ProducerSettings}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.compat.java8.FutureConverters._

/**
 * Utility class for producing to Kafka without using Akka Streams.
 */
final class SendProducer[K, V] private (underlying: scaladsl.SendProducer[K, V]) {

  /**
   * Utility class for producing to Kafka without using Akka Streams.
   * @param settings producer settings used to create or access the [[org.apache.kafka.clients.producer.Producer]]
   *
   * The internal asynchronous operations run on the provided `Executor` (which may be an `ActorSystem`'s dispatcher).
   */
  def this(settings: ProducerSettings[K, V], system: ActorSystem) =
    this(scaladsl.SendProducer(settings, system))

  /**
   * Utility class for producing to Kafka without using Akka Streams.
   * @param settings producer settings used to create or access the [[org.apache.kafka.clients.producer.Producer]]
   *
   * The internal asynchronous operations run on the provided `Executor` (which may be an `ActorSystem`'s dispatcher).
   */
  def this(settings: ProducerSettings[K, V], system: ClassicActorSystemProvider) =
    this(scaladsl.SendProducer(settings, system.classicSystem))

  /**
   * Send records to Kafka topics and complete a future with the result.
   *
   * It publishes records to Kafka topics conditionally:
   *
   * - [[akka.kafka.ProducerMessage.Message Message]] publishes a single message to its topic, and completes the future with [[akka.kafka.ProducerMessage.Result Result]]
   *
   * - [[akka.kafka.ProducerMessage.MultiMessage MultiMessage]] publishes all messages in its `records` field, and completes the future with [[akka.kafka.ProducerMessage.MultiResult MultiResult]]
   *
   * - [[akka.kafka.ProducerMessage.PassThroughMessage PassThroughMessage]] does not publish anything, and completes the future with [[akka.kafka.ProducerMessage.PassThroughResult PassThroughResult]]
   *
   * The messages support passing through arbitrary data.
   */
  def sendEnvelope[PT](envelope: Envelope[K, V, PT]): CompletionStage[Results[K, V, PT]] =
    underlying.sendEnvelope(envelope).toJava

  /**
   * Send a raw Kafka [[org.apache.kafka.clients.producer.ProducerRecord]] and complete a future with the resulting metadata.
   */
  def send(record: ProducerRecord[K, V]): CompletionStage[RecordMetadata] =
    underlying.send(record).toJava

  /**
   * Close the underlying producer (depending on the "close producer on stop" setting).
   */
  def close(): CompletionStage[Done] = underlying.close().toJava

  override def toString: String = s"SendProducer(${underlying.settings})"
}
