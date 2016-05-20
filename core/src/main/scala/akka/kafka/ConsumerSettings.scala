/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import akka.kafka.internal.ConfigSettings
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import java.util.concurrent.TimeUnit
import scala.annotation.varargs

object ConsumerSettings {

  /**
   * Create settings from the default configuration
   * `akka.kafka.consumer`.
   *
   * It will subscribe to the given `topics`, use empty `Set` if you
   * use manually assigned topics/partitions using [[ConsumerSettings#withAssignment]]
   * and/or [[ConsumerSettings#withFromOffset]].
   */
  def apply[K, V](
    system: ActorSystem,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    topics: Set[String]
  ): ConsumerSettings[K, V] =
    apply(system.settings.config.getConfig("akka.kafka.consumer"), keyDeserializer, valueDeserializer, topics)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   *
   * It will subscribe to the given `topics`, use empty `Set` if you
   * use manually assigned topics/partitions using [[ConsumerSettings#withAssignment]]
   * and/or [[ConsumerSettings#withFromOffset]].
   */
  def apply[K, V](
    config: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    topics: Set[String]
  ): ConsumerSettings[K, V] = {
    val properties = ConfigSettings.parseKafkaClientsProperties(config.getConfig("kafka-clients"))
    val pollInterval = config.getDuration("poll-interval", TimeUnit.MILLISECONDS).millis
    val pollTimeout = config.getDuration("poll-timeout", TimeUnit.MILLISECONDS).millis
    val pollCommitTimeout = config.getDuration("poll-commit-timeout", TimeUnit.MILLISECONDS).millis
    val stopTimeout = config.getDuration("stop-timeout", TimeUnit.MILLISECONDS).millis
    val closeTimeout = config.getDuration("close-timeout", TimeUnit.MILLISECONDS).millis
    val commitTimeout = config.getDuration("commit-timeout", TimeUnit.MILLISECONDS).millis
    val dispatcher = config.getString("use-dispatcher")
    new ConsumerSettings[K, V](properties, keyDeserializer, valueDeserializer, topics, Set.empty, Map.empty,
      pollInterval, pollTimeout, pollCommitTimeout, stopTimeout, closeTimeout, commitTimeout, dispatcher)
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.consumer`.
   *
   * It will subscribe to the given `topics`, use empty `Set` if you
   * use manually assigned topics/partitions using [[ConsumerSettings#withAssignment]]
   * and/or [[ConsumerSettings#withFromOffset]].
   */
  def create[K, V](
    system: ActorSystem,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    topics: java.util.Set[String]
  ): ConsumerSettings[K, V] =
    apply(system, keyDeserializer, valueDeserializer, topics.asScala.toSet)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.consumer`.
   *
   * It will subscribe to the given `topics`, use empty `Set` if you
   * use manually assigned topics/partitions using [[ConsumerSettings#withAssignment]]
   * and/or [[ConsumerSettings#withFromOffset]].
   */
  def create[K, V](
    config: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    topics: java.util.Set[String]
  ): ConsumerSettings[K, V] =
    apply(config, keyDeserializer, valueDeserializer, topics.asScala.toSet)

  /**
   * Java API: convenience to create a Set from varargs
   */
  @varargs def asSet(topics: String*): java.util.Set[String] = {
    val result = new java.util.HashSet[String]
    result.addAll(topics.asJava)
    result
  }

}

/**
 * Settings for consumers. See `akka.kafka.consumer` section in
 * reference.conf. Note that the [[ConsumerSettings$ companion]] object provides
 * `apply` and `create` functions for convenient construction of the settings, together with
 * the `with` methods.
 */
final class ConsumerSettings[K, V](
    val properties: Map[String, String],
    val keyDeserializer: Deserializer[K],
    val valueDeserializer: Deserializer[V],
    val topics: Set[String],
    val assignments: Set[TopicPartition],
    val fromOffsets: Map[TopicPartition, Long],
    val pollInterval: FiniteDuration,
    val pollTimeout: FiniteDuration,
    val pollCommitTimeout: FiniteDuration,
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

  def withAssignment(assignment: TopicPartition): ConsumerSettings[K, V] =
    copy(assignments = assignments + assignment)

  def withFromOffset(topicPartition: TopicPartition, offset: Long): ConsumerSettings[K, V] =
    copy(fromOffsets = fromOffsets.updated(topicPartition, offset))

  def withPollTimeout(pollTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(pollTimeout = pollTimeout)

  def withPollCommitTimeout(pollCommitTimeout: FiniteDuration): ConsumerSettings[K, V] =
    copy(pollCommitTimeout = pollCommitTimeout)

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
    keyDeserializer: Deserializer[K] = keyDeserializer,
    valueDeserializer: Deserializer[V] = valueDeserializer,
    topics: Set[String] = topics,
    assignments: Set[TopicPartition] = assignments,
    fromOffsets: Map[TopicPartition, Long] = fromOffsets,
    pollInterval: FiniteDuration = pollInterval,
    pollTimeout: FiniteDuration = pollTimeout,
    pollCommitTimeout: FiniteDuration = pollCommitTimeout,
    stopTimeout: FiniteDuration = stopTimeout,
    closeTimeout: FiniteDuration = closeTimeout,
    commitTimeout: FiniteDuration = commitTimeout,
    dispatcher: String = dispatcher
  ): ConsumerSettings[K, V] =
    new ConsumerSettings[K, V](properties, keyDeserializer, valueDeserializer,
      topics, assignments, fromOffsets,
      pollInterval, pollTimeout, pollCommitTimeout, stopTimeout, closeTimeout, commitTimeout,
      dispatcher)

  /**
   * Create a `KafkaConsumer` instance from the settings.
   * It will subscribe to the defined `topics` or set topic/partition
   * assignments. Either topics or assignments must be defined, not both.
   * It will `seek` to the defined `fromOffset` if any.
   */
  def createKafkaConsumer(): KafkaConsumer[K, V] = {
    val javaProps = properties.foldLeft(new java.util.Properties) {
      case (p, (k, v)) => p.put(k, v); p
    }
    val consumer = new KafkaConsumer[K, V](javaProps, keyDeserializer, valueDeserializer)
    val assignments2 = assignments union fromOffsets.keySet
    if (topics.nonEmpty && assignments2.nonEmpty)
      throw new IllegalArgumentException("Either topics or assignments must be defined, both of them were defined: $settings")
    else if (topics.nonEmpty)
      consumer.subscribe(topics.toList.asJava)
    else if (assignments2.nonEmpty)
      consumer.assign(assignments2.toList.asJava)
    else
      throw new IllegalArgumentException("Either topics or assignments must be defined, none of them were defined: $settings")

    if (fromOffsets.nonEmpty) {
      fromOffsets.foreach {
        case (partition, offset) =>
          consumer.seek(partition, offset)
      }
    }

    consumer
  }
}
