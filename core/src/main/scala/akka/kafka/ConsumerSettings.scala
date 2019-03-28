/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import java.util.Optional

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.kafka.internal._
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

object ConsumerSettings {

  val configPath = "akka.kafka.consumer"

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
    val config = system.settings.config.getConfig(configPath)
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
    val pollInterval = config.getDuration("poll-interval").asScala
    val pollTimeout = config.getDuration("poll-timeout").asScala
    val stopTimeout = config.getDuration("stop-timeout").asScala
    val closeTimeout = config.getDuration("close-timeout").asScala
    val commitTimeout = config.getDuration("commit-timeout").asScala
    val commitTimeWarning = config.getDuration("commit-time-warning").asScala
    val commitRefreshInterval = ConfigSettings.getPotentiallyInfiniteDuration(config, "commit-refresh-interval")
    val dispatcher = config.getString("use-dispatcher")
    val waitClosePartition = config.getDuration("wait-close-partition").asScala
    val positionTimeout = config.getDuration("position-timeout").asScala
    val offsetForTimesTimeout = config.getDuration("offset-for-times-timeout").asScala
    val metadataRequestTimeout = config.getDuration("metadata-request-timeout").asScala
    new ConsumerSettings[K, V](
      properties,
      keyDeserializer,
      valueDeserializer,
      pollInterval,
      pollTimeout,
      stopTimeout,
      closeTimeout,
      commitTimeout,
      commitRefreshInterval,
      dispatcher,
      commitTimeWarning,
      waitClosePartition,
      positionTimeout,
      offsetForTimesTimeout,
      metadataRequestTimeout,
      ConsumerSettings.createKafkaConsumer
    )
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
  ): ConsumerSettings[K, V] =
    apply(system, Option(keyDeserializer), Option(valueDeserializer))

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   * Key and value serializer must be passed explicitly.
   */
  def apply[K, V](
      config: Config,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] =
    apply(config, Option(keyDeserializer), Option(valueDeserializer))

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

  /**
   * Create a [[org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer]] instance from the settings.
   */
  def createKafkaConsumer[K, V](settings: ConsumerSettings[K, V]): Consumer[K, V] =
    new KafkaConsumer[K, V](settings.getProperties,
                            settings.keyDeserializerOpt.orNull,
                            settings.valueDeserializerOpt.orNull)

}

/**
 * Settings for consumers. See `akka.kafka.consumer` section in
 * `reference.conf`. Note that the [[akka.kafka.ConsumerSettings$ companion]] object provides
 * `apply` and `create` functions for convenient construction of the settings, together with
 * the `with` methods.
 *
 * The constructor is Internal API.
 */
class ConsumerSettings[K, V] @InternalApi private[kafka] (
    val properties: Map[String, String],
    val keyDeserializerOpt: Option[Deserializer[K]],
    val valueDeserializerOpt: Option[Deserializer[V]],
    val pollInterval: FiniteDuration,
    val pollTimeout: FiniteDuration,
    val stopTimeout: FiniteDuration,
    val closeTimeout: FiniteDuration,
    val commitTimeout: FiniteDuration,
    val commitRefreshInterval: Duration,
    val dispatcher: String,
    val commitTimeWarning: FiniteDuration,
    val waitClosePartition: FiniteDuration,
    val positionTimeout: FiniteDuration,
    val offsetForTimesTimeout: FiniteDuration,
    val metadataRequestTimeout: FiniteDuration,
    val consumerFactory: ConsumerSettings[K, V] => Consumer[K, V]
) {

  @deprecated("use the factory methods `ConsumerSettings.apply` and `create` instead", "1.0-M1")
  def this(properties: Map[String, String],
           keyDeserializer: Option[Deserializer[K]],
           valueDeserializer: Option[Deserializer[V]],
           pollInterval: FiniteDuration,
           pollTimeout: FiniteDuration,
           stopTimeout: FiniteDuration,
           closeTimeout: FiniteDuration,
           commitTimeout: FiniteDuration,
           wakeupTimeout: FiniteDuration,
           maxWakeups: Int,
           commitRefreshInterval: Duration,
           dispatcher: String,
           commitTimeWarning: FiniteDuration,
           wakeupDebug: Boolean,
           waitClosePartition: FiniteDuration) = this(
    properties,
    keyDeserializer,
    valueDeserializer,
    pollInterval,
    pollTimeout,
    stopTimeout,
    closeTimeout,
    commitTimeout,
    commitRefreshInterval,
    dispatcher,
    commitTimeWarning,
    waitClosePartition,
    positionTimeout = 5.seconds,
    offsetForTimesTimeout = 5.seconds,
    metadataRequestTimeout = 5.seconds,
    consumerFactory = ConsumerSettings.createKafkaConsumer
  )

  /**
   * A comma-separated list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
   */
  def withBootstrapServers(bootstrapServers: String): ConsumerSettings[K, V] =
    withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  /**
   * An id string to pass to the server when making requests. The purpose of this is to be able to track the source
   * of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.
   */
  def withClientId(clientId: String): ConsumerSettings[K, V] =
    withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

  /**
   * A unique string that identifies the consumer group this consumer belongs to.
   */
  def withGroupId(groupId: String): ConsumerSettings[K, V] =
    withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  /**
   * Scala API:
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.consumer.ConsumerConfig]].
   */
  def withProperties(properties: Map[String, String]): ConsumerSettings[K, V] =
    copy(properties = this.properties ++ properties)

  /**
   * Scala API:
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.consumer.ConsumerConfig]].
   */
  def withProperties(properties: (String, String)*): ConsumerSettings[K, V] =
    copy(properties = this.properties ++ properties.toMap)

  /**
   * Java API:
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.consumer.ConsumerConfig]].
   */
  def withProperties(properties: java.util.Map[String, String]): ConsumerSettings[K, V] =
    copy(properties = this.properties ++ properties.asScala)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.consumer.ConsumerConfig]].
   */
  def withProperty(key: String, value: String): ConsumerSettings[K, V] =
    copy(properties = properties.updated(key, value))

  /**
   * Java API: Get a raw property. `null` if it is not defined.
   */
  def getProperty(key: String): String = properties.getOrElse(key, null)

  /**
   * Set the maximum duration a poll to the Kafka broker is allowed to take.
   */
  def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(pollTimeout = pollTimeout)

  /**
   * Java API:
   * Set the maximum duration a poll to the Kafka broker is allowed to take.
   */
  def withPollTimeout(pollTimeout: java.time.Duration): ConsumerSettings[K, V] =
    copy(pollTimeout = pollTimeout.asScala)

  /**
   * Set the interval from one scheduled poll to the next.
   */
  def withPollInterval(pollInterval: FiniteDuration): ConsumerSettings[K, V] =
    copy(pollInterval = pollInterval)

  /**
   * Java API:
   * Set the interval from one scheduled poll to the next.
   */
  def withPollInterval(pollInterval: java.time.Duration): ConsumerSettings[K, V] =
    copy(pollInterval = pollInterval.asScala)

  /**
   * The stage will await outstanding offset commit requests before
   * shutting down, but if that takes longer than this timeout it will
   * stop forcefully.
   */
  def withStopTimeout(stopTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(stopTimeout = stopTimeout)

  /**
   * Java API:
   * The stage will await outstanding offset commit requests before
   * shutting down, but if that takes longer than this timeout it will
   * stop forcefully.
   */
  def withStopTimeout(stopTimeout: java.time.Duration): ConsumerSettings[K, V] =
    copy(stopTimeout = stopTimeout.asScala)

  /**
   * Set duration to wait for `KafkaConsumer.close` to finish.
   */
  def withCloseTimeout(closeTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(closeTimeout = closeTimeout)

  /**
   * Java API:
   * Set duration to wait for `KafkaConsumer.close` to finish.
   */
  def withCloseTimeout(closeTimeout: java.time.Duration): ConsumerSettings[K, V] =
    copy(closeTimeout = closeTimeout.asScala)

  /**
   * If offset commit requests are not completed within this timeout
   * the returned Future is completed with [[akka.kafka.CommitTimeoutException]].
   */
  def withCommitTimeout(commitTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(commitTimeout = commitTimeout)

  /**
   * Java API:
   * If offset commit requests are not completed within this timeout
   * the returned Future is completed with [[akka.kafka.CommitTimeoutException]].
   */
  def withCommitTimeout(commitTimeout: java.time.Duration): ConsumerSettings[K, V] =
    copy(commitTimeout = commitTimeout.asScala)

  /**
   * If commits take longer than this time a warning is logged
   */
  def withCommitWarning(commitTimeWarning: FiniteDuration): ConsumerSettings[K, V] =
    copy(commitTimeWarning = commitTimeWarning)

  /**
   * Java API:
   * If commits take longer than this time a warning is logged
   */
  def withCommitWarning(commitTimeWarning: java.time.Duration): ConsumerSettings[K, V] =
    copy(commitTimeWarning = commitTimeWarning.asScala)

  /**
   * Not used anymore
   * @deprecated not used anymore, since 1.0-RC1
   */
  @deprecated("not used anymore", "1.0-RC1")
  def withWakeupTimeout(wakeupTimeout: FiniteDuration): ConsumerSettings[K, V] = this

  /**
   * Java API:
   * Not used anymore
   * @deprecated not used anymore, since 1.0-RC1
   */
  @deprecated("not used anymore", "1.0-RC1")
  def withWakeupTimeout(wakeupTimeout: java.time.Duration): ConsumerSettings[K, V] = this

  /**
   * Fully qualified config path which holds the dispatcher configuration
   * to be used by the [[akka.kafka.KafkaConsumerActor]]. Some blocking may occur.
   */
  def withDispatcher(dispatcher: String): ConsumerSettings[K, V] =
    copy(dispatcher = dispatcher)

  /**
   * Not used anymore
   * @deprecated not used anymore, since 1.0-RC1
   */
  @deprecated("not used anymore", "1.0-RC1")
  def withMaxWakeups(maxWakeups: Int): ConsumerSettings[K, V] = this

  /**
   * If set to a finite duration, the consumer will re-send the last committed offsets periodically
   * for all assigned partitions.
   *
   * @see https://issues.apache.org/jira/browse/KAFKA-4682
   */
  def withCommitRefreshInterval(commitRefreshInterval: Duration): ConsumerSettings[K, V] =
    copy(commitRefreshInterval = commitRefreshInterval)

  /**
   * Java API:
   * If set to a finite duration, the consumer will re-send the last committed offsets periodically
   * for all assigned partitions. @see https://issues.apache.org/jira/browse/KAFKA-4682
   * Set to [[java.time.Duration.ZERO]] to switch it off.
   *
   * @see https://issues.apache.org/jira/browse/KAFKA-4682
   */
  def withCommitRefreshInterval(commitRefreshInterval: java.time.Duration): ConsumerSettings[K, V] =
    if (commitRefreshInterval.isZero) copy(commitRefreshInterval = Duration.Inf)
    else copy(commitRefreshInterval = commitRefreshInterval.asScala)

  /**
   * Not used anymore
   * @deprecated not used anymore, since 1.0-RC1
   */
  @deprecated("not used anymore", "1.0-RC1")
  def withWakeupDebug(wakeupDebug: Boolean): ConsumerSettings[K, V] = this

  /**
   * Time to wait for pending requests when a partition is closed.
   */
  def withWaitClosePartition(waitClosePartition: FiniteDuration): ConsumerSettings[K, V] =
    copy(waitClosePartition = waitClosePartition)

  /**
   * Java API:
   * Time to wait for pending requests when a partition is closed.
   */
  def withWaitClosePartition(waitClosePartition: java.time.Duration): ConsumerSettings[K, V] =
    copy(waitClosePartition = waitClosePartition.asScala)

  /** Scala API: Limits the blocking on Kafka consumer position calls. */
  def withPositionTimeout(positionTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(positionTimeout = positionTimeout)

  /** Java API: Limits the blocking on Kafka consumer position calls. */
  def withPositionTimeout(positionTimeout: java.time.Duration): ConsumerSettings[K, V] =
    copy(positionTimeout = positionTimeout.asScala)

  /** Scala API: Limits the blocking on Kafka consumer offsetForTimes calls. */
  def withOffsetForTimesTimeout(offsetForTimesTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(offsetForTimesTimeout = offsetForTimesTimeout)

  /** Java API: Limits the blocking on Kafka consumer offsetForTimes calls. */
  def withOffsetForTimesTimeout(offsetForTimesTimeout: java.time.Duration): ConsumerSettings[K, V] =
    copy(offsetForTimesTimeout = offsetForTimesTimeout.asScala)

  /** Scala API */
  def withMetadataRequestTimeout(metadataRequestTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(metadataRequestTimeout = metadataRequestTimeout)

  /** Java API */
  def withMetadataRequestTimeout(metadataRequestTimeout: java.time.Duration): ConsumerSettings[K, V] =
    copy(metadataRequestTimeout = metadataRequestTimeout.asScala)

  /**
   * Replaces the default Kafka consumer creation logic.
   */
  def withConsumerFactory(
      factory: ConsumerSettings[K, V] => Consumer[K, V]
  ): ConsumerSettings[K, V] = copy(consumerFactory = factory)

  /**
   * Get the Kafka consumer settings as map.
   */
  def getProperties: java.util.Map[String, AnyRef] = properties.asInstanceOf[Map[String, AnyRef]].asJava

  def getCloseTimeout: java.time.Duration = closeTimeout.asJava
  def getPositionTimeout: java.time.Duration = positionTimeout.asJava
  def getOffsetForTimesTimeout: java.time.Duration = offsetForTimesTimeout.asJava
  def getMetadataRequestTimeout: java.time.Duration = metadataRequestTimeout.asJava

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
      commitRefreshInterval: Duration = commitRefreshInterval,
      dispatcher: String = dispatcher,
      waitClosePartition: FiniteDuration = waitClosePartition,
      positionTimeout: FiniteDuration = positionTimeout,
      offsetForTimesTimeout: FiniteDuration = offsetForTimesTimeout,
      metadataRequestTimeout: FiniteDuration = metadataRequestTimeout,
      consumerFactory: ConsumerSettings[K, V] => Consumer[K, V] = consumerFactory
  ): ConsumerSettings[K, V] =
    new ConsumerSettings[K, V](
      properties,
      keyDeserializer,
      valueDeserializer,
      pollInterval,
      pollTimeout,
      stopTimeout,
      closeTimeout,
      commitTimeout,
      commitRefreshInterval,
      dispatcher,
      commitTimeWarning,
      waitClosePartition,
      positionTimeout,
      offsetForTimesTimeout,
      metadataRequestTimeout,
      consumerFactory
    )

  /**
   * Create a [[org.apache.kafka.clients.consumer.Consumer Kafka Consumer]] instance from these settings.
   */
  def createKafkaConsumer(): Consumer[K, V] = consumerFactory.apply(this)

  override def toString: String =
    s"akka.kafka.ConsumerSettings(" +
    s"properties=${properties.mkString(",")}," +
    s"keyDeserializer=$keyDeserializerOpt," +
    s"valueDeserializer=$valueDeserializerOpt," +
    s"pollInterval=${pollInterval.toCoarsest}," +
    s"pollTimeout=${pollTimeout.toCoarsest}," +
    s"stopTimeout=${stopTimeout.toCoarsest}," +
    s"closeTimeout=${closeTimeout.toCoarsest}," +
    s"commitTimeout=${commitTimeout.toCoarsest}," +
    s"commitRefreshInterval=${commitRefreshInterval.toCoarsest}," +
    s"dispatcher=$dispatcher," +
    s"commitTimeWarning=${commitTimeWarning.toCoarsest}," +
    s"waitClosePartition=${waitClosePartition.toCoarsest}," +
    s"metadataRequestTimeout=${metadataRequestTimeout.toCoarsest}," +
    ")"
}
