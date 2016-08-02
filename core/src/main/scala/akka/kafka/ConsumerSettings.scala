/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.internal.ConfigSettings
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer

import scala.annotation.varargs
import scala.concurrent.duration._
import scala.collection.JavaConverters._

sealed trait Subscription
sealed trait ManualSubscription extends Subscription
sealed trait AutoSubscription extends Subscription

object Subscriptions {
  private[kafka] final case class TopicSubscription(tps: Set[String]) extends AutoSubscription
  private[kafka] final case class TopicSubscriptionPattern(pattern: String) extends AutoSubscription
  private[kafka] final case class Assignment(tps: Set[TopicPartition]) extends ManualSubscription
  private[kafka] final case class AssignmentWithOffset(tps: Map[TopicPartition, Long]) extends ManualSubscription

  /**
   * Creates subscription for given set of topics
   */
  def topics(ts: Set[String]): AutoSubscription = TopicSubscription(ts)

  /**
   * Creates subscription for given set of topics
   * JAVA API
   */
  @varargs
  def topics(ts: String*): AutoSubscription = topics(ts.toSet)

  /**
   * Creates subscription for given set of topics
   * JAVA API
   */
  def topics(ts: java.util.Set[String]): AutoSubscription = topics(ts.asScala.toSet)

  /**
   * Creates subscription for given topics pattern
   */
  def topicPattern(pattern: String): AutoSubscription = TopicSubscriptionPattern(pattern)

  /**
   * Manually assign given topics and partitions
   */
  def assignment(tps: Set[TopicPartition]): ManualSubscription = Assignment(tps)

  /**
   * Manually assign given topics and partitions
   * JAVA API
   */
  @varargs
  def assignment(tps: TopicPartition*): ManualSubscription = assignment(tps.toSet)

  /**
   * Manually assign given topics and partitions
   * JAVA API
   */
  def assignment(tps: java.util.Set[TopicPartition]): ManualSubscription = assignment(tps.asScala.toSet)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tps: Map[TopicPartition, Long]): ManualSubscription = AssignmentWithOffset(tps)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tps: (TopicPartition, Long)*): ManualSubscription = AssignmentWithOffset(tps.toMap)

  /**
   * Manually assign given topics and partitions with offsets
   * JAVA API
   */
  def assignmentWithOffset(tps: java.util.Map[TopicPartition, Long]): ManualSubscription = assignmentWithOffset(tps.asScala.toMap)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tp: TopicPartition, offset: Long): ManualSubscription = assignmentWithOffset(Map(tp -> offset))
}

object ConsumerSettings {

  def apply[K, V](
    system: ActorSystem,
    keyDeserializer: Option[Deserializer[K]],
    valueDeserializer: Option[Deserializer[V]]
  ): ConsumerSettings[K, V] = {
    val config = system.settings.config.getConfig("akka.kafka.consumer")
    apply(config, keyDeserializer, valueDeserializer)
  }

  def apply[K, V](
    config: Config,
    keyDeserializer: Option[Deserializer[K]],
    valueDeserializer: Option[Deserializer[V]]
  ): ConsumerSettings[K, V] = {
    val properties = ConfigSettings.parseKafkaClientsProperties(config.getConfig("kafka-clients"))
    require(
      keyDeserializer.isDefined || properties.contains(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG),
      "Key deserializer should be defined or declared in configuration"
    )
    require(
      valueDeserializer.isDefined || properties.contains(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG),
      "Value deserializer should be defined or declared in configuration"
    )
    val pollInterval = config.getDuration("poll-interval", TimeUnit.MILLISECONDS).millis
    val pollTimeout = config.getDuration("poll-timeout", TimeUnit.MILLISECONDS).millis
    val stopTimeout = config.getDuration("stop-timeout", TimeUnit.MILLISECONDS).millis
    val closeTimeout = config.getDuration("close-timeout", TimeUnit.MILLISECONDS).millis
    val commitTimeout = config.getDuration("commit-timeout", TimeUnit.MILLISECONDS).millis
    val dispatcher = config.getString("use-dispatcher")
    new ConsumerSettings[K, V](properties, keyDeserializer, valueDeserializer,
      pollInterval, pollTimeout, stopTimeout, closeTimeout, commitTimeout, dispatcher)
  }

  def apply[K, V](
    system: ActorSystem
  ): ConsumerSettings[K, V] = {
    apply(system, None, None)
  }

  def apply[K, V](
    config: Config
  ): ConsumerSettings[K, V] = {
    apply(config, None, None)
  }

  /**
   * Create settings from the default configuration
   * `akka.kafka.consumer`.
   */
  def apply[K, V](
    system: ActorSystem,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] = {
    apply(system, Some(keyDeserializer), Some(valueDeserializer))
  }

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   */
  def apply[K, V](
    config: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] = {
    apply(config, Some(keyDeserializer), Some(valueDeserializer))
  }

  def create[K, V](
    system: ActorSystem
  ): ConsumerSettings[K, V] = {
    apply(system, None, None)
  }

  def create[K, V](
     config: Config
   ): ConsumerSettings[K, V] = {
    apply(config, None, None)
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.consumer`.
   */
  def create[K, V](
    system: ActorSystem,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] =
    apply(system, Option(keyDeserializer), Option(valueDeserializer))

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   */
  def create[K, V](
    config: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] =
    apply(config, Option(keyDeserializer), Option(valueDeserializer))
}

/**
 * Settings for consumers. See `akka.kafka.consumer` section in
 * reference.conf. Note that the [[ConsumerSettings companion]] object provides
 * `apply` and `create` functions for convenient construction of the settings, together with
 * the `with` methods.
 */
class ConsumerSettings[K, V](
    val properties: Map[String, String],
    val keyDeserializerOpt: Option[Deserializer[K]],
    val valueDeserializerOpt: Option[Deserializer[V]],
    val pollInterval: FiniteDuration,
    val pollTimeout: FiniteDuration,
    val stopTimeout: FiniteDuration,
    val closeTimeout: FiniteDuration,
    val commitTimeout: FiniteDuration,
    val dispatcher: String
) {

  def withBootstrapServers(bootstrapServers: String): ConsumerSettings[K, V] =
    withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  def withClientId(clientId: String): ConsumerSettings[K, V] =
    withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

  def withGroupId(groupId: String): ConsumerSettings[K, V] =
    withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * `org.apache.kafka.clients.consumer.ConsumerConfig`.
   */
  def withProperty(key: String, value: String): ConsumerSettings[K, V] =
    copy(properties = properties.updated(key, value))

  /**
   * Java API: Get a raw property. `null` if it is not defined.
   */
  def getProperty(key: String): String = properties.getOrElse(key, null)

  def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(pollTimeout = pollTimeout)

  def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[K, V] =
    copy(pollInterval = pollInterval)

  def withStopTimeout(stopTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(stopTimeout = stopTimeout)

  def withCloseTimeout(closeTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(closeTimeout = closeTimeout)

  def withCommitTimeout(commitTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(commitTimeout = commitTimeout)

  def withDispatcher(dispatcher: String): ConsumerSettings[K, V] =
    copy(dispatcher = dispatcher)

  private def copy(
    properties: Map[String, String] = properties,
    keyDeserializer: Option[Deserializer[K]] = keyDeserializerOpt,
    valueDeserializer: Option[Deserializer[V]] = valueDeserializerOpt,
    pollInterval: FiniteDuration = pollInterval,
    pollTimeout: FiniteDuration = pollTimeout,
    stopTimeout: FiniteDuration = stopTimeout,
    closeTimeout: FiniteDuration = closeTimeout,
    commitTimeout: FiniteDuration = commitTimeout,
    dispatcher: String = dispatcher
  ): ConsumerSettings[K, V] =
    new ConsumerSettings[K, V](properties, keyDeserializer, valueDeserializer,
      pollInterval, pollTimeout, stopTimeout, closeTimeout, commitTimeout,
      dispatcher)

  /**
   * Create a `KafkaConsumer` instance from the settings.
   */
  def createKafkaConsumer(): KafkaConsumer[K, V] = {
    val javaProps = properties.foldLeft(new java.util.Properties) {
      case (p, (k, v)) => p.put(k, v); p
    }
    val deserializers = for {
      keyDeserializer <- keyDeserializerOpt
      valueDeserializer <- valueDeserializerOpt
    } yield {
      keyDeserializer -> valueDeserializer
    }
    deserializers match {
      case Some((keyDeserializer, valueDeserializer)) => new KafkaConsumer[K, V](javaProps, keyDeserializer, valueDeserializer)
      case None => new KafkaConsumer[K, V](javaProps)
    }
  }
}
