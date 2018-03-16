/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import java.util.Optional
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.internal.ConfigSettings
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

sealed trait Subscription
sealed trait ManualSubscription extends Subscription
sealed trait AutoSubscription extends Subscription

object Subscriptions {
  private[kafka] final case class TopicSubscriptionWithStartTimestamp(timestamp: Long, tps: Set[String]) extends AutoSubscription
  private[kafka] final case class TopicSubscription(tps: Set[String]) extends AutoSubscription
  private[kafka] final case class TopicSubscriptionPattern(pattern: String) extends AutoSubscription
  private[kafka] final case class Assignment(tps: Set[TopicPartition]) extends ManualSubscription
  private[kafka] final case class AssignmentWithOffset(tps: Map[TopicPartition, Long]) extends ManualSubscription
  private[kafka] final case class AssignmentOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) extends ManualSubscription

  /**
   * Creates subscription starting from `timestamp` for given set of topics
   */
  def topicsWithStartTimestamp(timestamp: Long, ts: Set[String]): AutoSubscription = TopicSubscriptionWithStartTimestamp(timestamp, ts)

  /**
   * Creates subscription starting from `timestamp` for given set of topics
   * JAVA API
   */
  @varargs
  def topics(timestamp: Long, ts: String*): AutoSubscription = topicsWithStartTimestamp(timestamp, ts.toSet)

  /**
   * Creates subscription starting from `timestamp` for given set of topics
   * JAVA API
   */
  def topics(timestamp: Long, ts: java.util.Set[String]): AutoSubscription = topicsWithStartTimestamp(timestamp, ts.asScala.toSet)

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
  def assignmentWithOffset(tps: java.util.Map[TopicPartition, java.lang.Long]): ManualSubscription = assignmentWithOffset(tps.asScala.toMap.asInstanceOf[Map[TopicPartition, Long]])

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentWithOffset(tp: TopicPartition, offset: Long): ManualSubscription = assignmentWithOffset(Map(tp -> offset))

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentOffsetsForTimes(tps: Map[TopicPartition, Long]): ManualSubscription = AssignmentOffsetsForTimes(tps)

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentOffsetsForTimes(tps: (TopicPartition, Long)*): ManualSubscription = AssignmentOffsetsForTimes(tps.toMap)

  /**
   * Manually assign given topics and partitions with offsets
   * JAVA API
   */
  def assignmentOffsetsForTimes(tps: java.util.Map[TopicPartition, java.lang.Long]): ManualSubscription = assignmentOffsetsForTimes(tps.asScala.toMap.asInstanceOf[Map[TopicPartition, Long]])

  /**
   * Manually assign given topics and partitions with offsets
   */
  def assignmentOffsetsForTimes(tp: TopicPartition, timestamp: Long): ManualSubscription = assignmentOffsetsForTimes(Map(tp -> timestamp))

}

object ConsumerSettings {

  /**
   * Create settings from the default configuration
   * `akka.kafka.consumer`.
   * Key or value deserializer can be passed explicitly or retrieved from configuration.
   */
  def apply[K, V](
    system: ActorSystem,
    keyDeserializer: Option[Deserializer[K]],
    valueDeserializer: Option[Deserializer[V]]
  ): ConsumerSettings[K, V] = {
    val config = system.settings.config.getConfig("akka.kafka.consumer")
    apply(config, keyDeserializer, valueDeserializer)
  }

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   * Key or value deserializer can be passed explicitly or retrieved from configuration.
   */
  def apply[K, V](
    config: Config,
    keyDeserializer: Option[Deserializer[K]],
    valueDeserializer: Option[Deserializer[V]]
  ): ConsumerSettings[K, V] = {
    val properties = ConfigSettings.parseKafkaClientsProperties(config.getConfig("kafka-clients"))
    require(
      keyDeserializer != null &&
        (keyDeserializer.isDefined || properties.contains(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)),
      "Key deserializer should be defined or declared in configuration"
    )
    require(
      valueDeserializer != null &&
        (valueDeserializer.isDefined || properties.contains(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)),
      "Value deserializer should be defined or declared in configuration"
    )
    val pollInterval = config.getDuration("poll-interval", TimeUnit.MILLISECONDS).millis
    val pollTimeout = config.getDuration("poll-timeout", TimeUnit.MILLISECONDS).millis
    val stopTimeout = config.getDuration("stop-timeout", TimeUnit.MILLISECONDS).millis
    val closeTimeout = config.getDuration("close-timeout", TimeUnit.MILLISECONDS).millis
    val commitTimeout = config.getDuration("commit-timeout", TimeUnit.MILLISECONDS).millis
    val commitTimeWarning = config.getDuration("commit-time-warning", TimeUnit.MILLISECONDS).millis
    val wakeupTimeout = config.getDuration("wakeup-timeout", TimeUnit.MILLISECONDS).millis
    val maxWakeups = config.getInt("max-wakeups")
    val dispatcher = config.getString("use-dispatcher")
    val wakeupDebug = config.getBoolean("wakeup-debug")
    new ConsumerSettings[K, V](properties, keyDeserializer, valueDeserializer,
      pollInterval, pollTimeout, stopTimeout, closeTimeout, commitTimeout, wakeupTimeout, maxWakeups, dispatcher,
      commitTimeWarning, wakeupDebug)
  }

  /**
   * Create settings from the default configuration
   * `akka.kafka.consumer`.
   * Key and value serializer must be passed explicitly.
   */
  def apply[K, V](
    system: ActorSystem,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] = {
    apply(system, Option(keyDeserializer), Option(valueDeserializer))
  }

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   * Key and value serializer must be passed explicitly.
   */
  def apply[K, V](
    config: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] = {
    apply(config, Option(keyDeserializer), Option(valueDeserializer))
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.consumer`.
   * Key or value deserializer can be passed explicitly or retrieved from configuration.
   */
  def create[K, V](
    system: ActorSystem,
    keyDeserializer: Optional[Deserializer[K]],
    valueDeserializer: Optional[Deserializer[V]]
  ): ConsumerSettings[K, V] =
    apply(system, keyDeserializer.asScala, valueDeserializer.asScala)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   * Key or value deserializer can be passed explicitly or retrieved from configuration.
   */
  def create[K, V](
    config: Config,
    keyDeserializer: Optional[Deserializer[K]],
    valueDeserializer: Optional[Deserializer[V]]
  ): ConsumerSettings[K, V] =
    apply(config, keyDeserializer.asScala, valueDeserializer.asScala)

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.consumer`.
   * Key and value serializer must be passed explicitly.
   */
  def create[K, V](
    system: ActorSystem,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] =
    apply(system, keyDeserializer, valueDeserializer)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   * Key and value serializer must be passed explicitly.
   */
  def create[K, V](
    config: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] =
    apply(config, keyDeserializer, valueDeserializer)
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
    val wakeupTimeout: FiniteDuration,
    val maxWakeups: Int,
    val dispatcher: String,
    val commitTimeWarning: FiniteDuration = 1.second,
    val wakeupDebug: Boolean = true
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
  def withProperties(properties: Map[String, String]): ConsumerSettings[K, V] =
    copy(properties = this.properties ++ properties)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * `org.apache.kafka.clients.consumer.ConsumerConfig`.
   */
  def withProperties(properties: (String, String)*): ConsumerSettings[K, V] =
    copy(properties = this.properties ++ properties.toMap)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * `org.apache.kafka.clients.consumer.ConsumerConfig`.
   */
  def withProperties(properties: java.util.Map[String, String]): ConsumerSettings[K, V] =
    copy(properties = this.properties ++ properties.asScala)

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

  def withCommitWarning(commitTimeWarning: FiniteDuration): ConsumerSettings[K, V] =
    copy(commitTimeWarning = commitTimeWarning)

  def withWakeupTimeout(wakeupTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(wakeupTimeout = wakeupTimeout)

  def withDispatcher(dispatcher: String): ConsumerSettings[K, V] =
    copy(dispatcher = dispatcher)

  def withMaxWakeups(maxWakeups: Int): ConsumerSettings[K, V] =
    copy(maxWakeups = maxWakeups)

  def withWakeupDebug(wakeupDebug: Boolean): ConsumerSettings[K, V] =
    copy(wakeupDebug = wakeupDebug)

  private def copy(
    properties: Map[String, String] = properties,
    keyDeserializer: Option[Deserializer[K]] = keyDeserializerOpt,
    valueDeserializer: Option[Deserializer[V]] = valueDeserializerOpt,
    pollInterval: FiniteDuration = pollInterval,
    pollTimeout: FiniteDuration = pollTimeout,
    stopTimeout: FiniteDuration = stopTimeout,
    closeTimeout: FiniteDuration = closeTimeout,
    commitTimeout: FiniteDuration = commitTimeout,
    commitTimeWarning: FiniteDuration = commitTimeWarning,
    wakeupTimeout: FiniteDuration = wakeupTimeout,
    maxWakeups: Int = maxWakeups,
    dispatcher: String = dispatcher,
    wakeupDebug: Boolean = wakeupDebug
  ): ConsumerSettings[K, V] =
    new ConsumerSettings[K, V](properties, keyDeserializer, valueDeserializer,
      pollInterval, pollTimeout, stopTimeout, closeTimeout, commitTimeout, wakeupTimeout,
      maxWakeups, dispatcher, commitTimeWarning, wakeupDebug)

  /**
   * Create a `KafkaConsumer` instance from the settings.
   */
  def createKafkaConsumer(): Consumer[K, V] = {
    val javaProps = properties.foldLeft(new java.util.Properties) {
      case (p, (k, v)) => p.put(k, v); p
    }
    new KafkaConsumer[K, V](javaProps, keyDeserializerOpt.orNull, valueDeserializerOpt.orNull)
  }
}
