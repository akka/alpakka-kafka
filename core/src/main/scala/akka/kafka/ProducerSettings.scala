/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import java.util.Optional

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.kafka.internal.ConfigSettings
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.Serializer

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

object ProducerSettings {

  val configPath = "akka.kafka.producer"

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
    apply(system.settings.config.getConfig(configPath), keySerializer, valueSerializer)

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
    val closeTimeout = config.getDuration("close-timeout").asScala
    val parallelism = config.getInt("parallelism")
    val dispatcher = config.getString("use-dispatcher")
    val eosCommitInterval = config.getDuration("eos-commit-interval").asScala
    new ProducerSettings[K, V](properties,
                               keySerializer,
                               valueSerializer,
                               closeTimeout,
                               parallelism,
                               dispatcher,
                               eosCommitInterval,
                               ProducerSettings.createKafkaProducer)
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
  ): ProducerSettings[K, V] =
    apply(config, Option(keySerializer), Option(valueSerializer))

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

  /**
   * Create a [[org.apache.kafka.clients.producer.KafkaProducer KafkaProducer]] instance from the settings.
   */
  def createKafkaProducer[K, V](settings: ProducerSettings[K, V]): KafkaProducer[K, V] =
    new KafkaProducer[K, V](settings.getProperties,
                            settings.keySerializerOpt.orNull,
                            settings.valueSerializerOpt.orNull)

}

/**
 * Settings for producers. See `akka.kafka.producer` section in
 * reference.conf. Note that the [[akka.kafka.ProducerSettings$ companion]] object provides
 * `apply` and `create` functions for convenient construction of the settings, together with
 * the `with` methods.
 *
 * The constructor is Internal API.
 */
class ProducerSettings[K, V] @InternalApi private[kafka] (
    val properties: Map[String, String],
    val keySerializerOpt: Option[Serializer[K]],
    val valueSerializerOpt: Option[Serializer[V]],
    val closeTimeout: FiniteDuration,
    val parallelism: Int,
    val dispatcher: String,
    val eosCommitInterval: FiniteDuration,
    val producerFactory: ProducerSettings[K, V] => Producer[K, V]
) {

  @deprecated("use the factory methods `ProducerSettings.apply` and `create` instead", "1.0-M1")
  def this(
      properties: Map[String, String],
      keySerializerOpt: Option[Serializer[K]],
      valueSerializerOpt: Option[Serializer[V]],
      closeTimeout: FiniteDuration,
      parallelism: Int,
      dispatcher: String,
      eosCommitInterval: FiniteDuration
  ) =
    this(properties,
         keySerializerOpt,
         valueSerializerOpt,
         closeTimeout,
         parallelism,
         dispatcher,
         eosCommitInterval,
         ProducerSettings.createKafkaProducer)

  /**
   * A comma-separated list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
   */
  def withBootstrapServers(bootstrapServers: String): ProducerSettings[K, V] =
    withProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  /**
   * Scala API:
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.producer.ProducerConfig]].
   */
  def withProperties(properties: Map[String, String]): ProducerSettings[K, V] =
    copy(properties = this.properties ++ properties)

  /**
   * Scala API:
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.producer.ProducerConfig]].
   */
  def withProperties(properties: (String, String)*): ProducerSettings[K, V] =
    copy(properties = this.properties ++ properties.toMap)

  /**
   * Java API:
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.producer.ProducerConfig]].
   */
  def withProperties(properties: java.util.Map[String, String]): ProducerSettings[K, V] =
    copy(properties = this.properties ++ properties.asScala)

  /**
   * The raw properties of the kafka-clients driver, see constants in
   * [[org.apache.kafka.clients.producer.ProducerConfig]].
   */
  def withProperty(key: String, value: String): ProducerSettings[K, V] =
    copy(properties = properties.updated(key, value))

  /**
   * Duration to wait for `KafkaConsumer.close` to finish.
   */
  def withCloseTimeout(closeTimeout: FiniteDuration): ProducerSettings[K, V] =
    copy(closeTimeout = closeTimeout)

  /**
   * Java API:
   * Duration to wait for `KafkaConsumer.close` to finish.
   */
  def withCloseTimeout(closeTimeout: java.time.Duration): ProducerSettings[K, V] =
    copy(closeTimeout = closeTimeout.asScala)

  /**
   * Tuning parameter of how many sends that can run in parallel.
   */
  def withParallelism(parallelism: Int): ProducerSettings[K, V] =
    copy(parallelism = parallelism)

  /**
   * Fully qualified config path which holds the dispatcher configuration
   * to be used by the producer stages. Some blocking may occur.
   * When this value is empty, the dispatcher configured for the stream
   * will be used.
   */
  def withDispatcher(dispatcher: String): ProducerSettings[K, V] =
    copy(dispatcher = dispatcher)

  /**
   * The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`.
   */
  def withEosCommitInterval(eosCommitInterval: FiniteDuration): ProducerSettings[K, V] =
    copy(eosCommitInterval = eosCommitInterval)

  /**
   * Java API:
   * The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`.
   */
  def withEosCommitInterval(eosCommitInterval: java.time.Duration): ProducerSettings[K, V] =
    copy(eosCommitInterval = eosCommitInterval.asScala)

  /**
   * Replaces the default Kafka producer creation logic.
   */
  def withProducerFactory(
      factory: ProducerSettings[K, V] => Producer[K, V]
  ): ProducerSettings[K, V] = copy(producerFactory = factory)

  /**
   * Get the Kafka producer settings as map.
   */
  def getProperties: java.util.Map[String, AnyRef] = properties.asInstanceOf[Map[String, AnyRef]].asJava

  private def copy(
      properties: Map[String, String] = properties,
      keySerializer: Option[Serializer[K]] = keySerializerOpt,
      valueSerializer: Option[Serializer[V]] = valueSerializerOpt,
      closeTimeout: FiniteDuration = closeTimeout,
      parallelism: Int = parallelism,
      dispatcher: String = dispatcher,
      eosCommitInterval: FiniteDuration = eosCommitInterval,
      producerFactory: ProducerSettings[K, V] => Producer[K, V] = producerFactory
  ): ProducerSettings[K, V] =
    new ProducerSettings[K, V](properties,
                               keySerializer,
                               valueSerializer,
                               closeTimeout,
                               parallelism,
                               dispatcher,
                               eosCommitInterval,
                               producerFactory)

  override def toString: String =
    "akka.kafka.ProducerSettings(" +
    s"properties=${properties.mkString(",")}," +
    s"keySerializer=$keySerializerOpt," +
    s"valueSerializer=$valueSerializerOpt," +
    s"closeTimeout=${closeTimeout.toCoarsest}," +
    s"parallelism=$parallelism," +
    s"dispatcher=$dispatcher," +
    s"eosCommitInterval=${eosCommitInterval.toCoarsest}" +
    ")"

  /**
   * Create a `Producer` instance from the settings.
   */
  def createKafkaProducer(): Producer[K, V] = producerFactory.apply(this)
}
