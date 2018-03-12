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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

object ProducerSettings {

  /**
   * Create settings from the default configuration
   * `akka.kafka.producer`.
   * Key or value serializer can be passed explicitly or retrieved from configuration.
   */
  def apply[K, V](
    system: ActorSystem,
    keySerializer: Option[Serializer[K]],
    valueSerializer: Option[Serializer[V]]
  ): ProducerSettings[K, V] =
    apply(system.settings.config.getConfig("akka.kafka.producer"), keySerializer, valueSerializer)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.producer`.
   * Key or value serializer can be passed explicitly or retrieved from configuration.
   */
  def apply[K, V](
    config: Config,
    keySerializer: Option[Serializer[K]],
    valueSerializer: Option[Serializer[V]]
  ): ProducerSettings[K, V] = {
    val properties = ConfigSettings.parseKafkaClientsProperties(config.getConfig("kafka-clients"))
    require(
      keySerializer != null &&
        (keySerializer.isDefined || properties.contains(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)),
      "Key serializer should be defined or declared in configuration"
    )
    require(
      valueSerializer != null &&
        (valueSerializer.isDefined || properties.contains(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)),
      "Value serializer should be defined or declared in configuration"
    )
    val closeTimeout = config.getDuration("close-timeout", TimeUnit.MILLISECONDS).millis
    val parallelism = config.getInt("parallelism")
    val dispatcher = config.getString("use-dispatcher")
    new ProducerSettings[K, V](properties, keySerializer, valueSerializer, closeTimeout, parallelism, dispatcher)
  }

  /**
   * Create settings from the default configuration
   * `akka.kafka.producer`.
   * Key and value serializer must be passed explicitly.
   */
  def apply[K, V](
    system: ActorSystem,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] =
    apply(system, Option(keySerializer), Option(valueSerializer))

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.producer`.
   * Key and value serializer must be passed explicitly.
   */
  def apply[K, V](
    config: Config,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] = {
    apply(config, Option(keySerializer), Option(valueSerializer))
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.producer`.
   * Key or value serializer can be passed explicitly or retrieved from configuration.
   */
  def create[K, V](
    system: ActorSystem,
    keySerializer: Optional[Serializer[K]],
    valueSerializer: Optional[Serializer[V]]
  ): ProducerSettings[K, V] =
    apply(system, keySerializer.asScala, valueSerializer.asScala)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.producer`.
   * Key or value serializer can be passed explicitly or retrieved from configuration.
   */
  def create[K, V](
    config: Config,
    keySerializer: Optional[Serializer[K]],
    valueSerializer: Optional[Serializer[V]]
  ): ProducerSettings[K, V] =
    apply(config, keySerializer.asScala, valueSerializer.asScala)

  /**
   * Java API: Create settings from the default configuration
   * `akka.kafka.producer`.
   * Key and value serializer must be passed explicitly.
   */
  def create[K, V](
    system: ActorSystem,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] =
    apply(system, keySerializer, valueSerializer)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.kafka.producer`.
   * Key and value serializer must be passed explicitly.
   */
  def create[K, V](
    config: Config,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] =
    apply(config, keySerializer, valueSerializer)

}

/**
 * Settings for producers. See `akka.kafka.producer` section in
 * reference.conf. Note that the [[ProducerSettings companion]] object provides
 * `apply` and `create` functions for convenient construction of the settings, together with
 * the `with` methods.
 */
final class ProducerSettings[K, V](
    val properties: Map[String, String],
    val keySerializerOpt: Option[Serializer[K]],
    val valueSerializerOpt: Option[Serializer[V]],
    val closeTimeout: FiniteDuration,
    val parallelism: Int,
    val dispatcher: String
) {

  def withBootstrapServers(bootstrapServers: String): ProducerSettings[K, V] =
    withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * `org.apache.kafka.clients.producer.ProducerConfig`.
   */
  def withProperties(properties: Map[String, String]): ProducerSettings[K, V] =
    copy(properties = this.properties ++ properties)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * `org.apache.kafka.clients.producer.ProducerConfig`.
   */
  def withProperties(properties: (String, String)*): ProducerSettings[K, V] =
    copy(properties = this.properties ++ properties.toMap)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * `org.apache.kafka.clients.producer.ProducerConfig`.
   */
  def withProperties(properties: java.util.Map[String, String]): ProducerSettings[K, V] =
    copy(properties = this.properties ++ properties.asScala)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * `org.apache.kafka.clients.producer.ProducerConfig`.
   */
  def withProperty(key: String, value: String): ProducerSettings[K, V] =
    copy(properties = properties.updated(key, value))

  def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[K, V] =
    copy(closeTimeout = closeTimeout)

  def withParallelism(parallelism: Int): ProducerSettings[K, V] =
    copy(parallelism = parallelism)

  def withDispatcher(dispatcher: String): ProducerSettings[K, V] =
    copy(dispatcher = dispatcher)

  private def copy(
    properties: Map[String, String] = properties,
    keySerializer: Option[Serializer[K]] = keySerializerOpt,
    valueSerializer: Option[Serializer[V]] = valueSerializerOpt,
    closeTimeout: FiniteDuration = closeTimeout,
    parallelism: Int = parallelism,
    dispatcher: String = dispatcher
  ): ProducerSettings[K, V] =
    new ProducerSettings[K, V](properties, keySerializer, valueSerializer, closeTimeout, parallelism, dispatcher)

  /**
   * Create a `KafkaProducer` instance from the settings.
   */
  def createKafkaProducer(): KafkaProducer[K, V] = {
    val javaProps = properties.foldLeft(new java.util.Properties) {
      case (p, (k, v)) => p.put(k, v); p
    }
    new KafkaProducer[K, V](javaProps, keySerializerOpt.orNull, valueSerializerOpt.orNull)
  }
}
