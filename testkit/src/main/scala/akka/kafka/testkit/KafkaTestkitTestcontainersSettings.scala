/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit

import java.time.Duration
import java.util.function.Consumer

import akka.actor.ActorSystem
import akka.kafka.testkit.internal.AlpakkaKafkaContainer
import com.typesafe.config.Config
import org.testcontainers.containers.GenericContainer
import scala.jdk.DurationConverters._
import scala.concurrent.duration.FiniteDuration

final class KafkaTestkitTestcontainersSettings private (
    val zooKeeperImage: String,
    val zooKeeperImageTag: String,
    val kafkaImage: String,
    val kafkaImageTag: String,
    val schemaRegistryImage: String,
    val schemaRegistryImageTag: String,
    val numBrokers: Int,
    val internalTopicsReplicationFactor: Int,
    val useSchemaRegistry: Boolean,
    val containerLogging: Boolean,
    val clusterStartTimeout: FiniteDuration,
    val readinessCheckTimeout: FiniteDuration,
    val configureKafka: Vector[AlpakkaKafkaContainer] => Unit = _ => (),
    val configureKafkaConsumer: java.util.function.Consumer[java.util.Collection[AlpakkaKafkaContainer]] =
      new Consumer[java.util.Collection[AlpakkaKafkaContainer]]() {
        override def accept(arg: java.util.Collection[AlpakkaKafkaContainer]): Unit = ()
      },
    val configureZooKeeper: GenericContainer[_] => Unit = _ => (),
    val configureZooKeeperConsumer: java.util.function.Consumer[GenericContainer[_]] =
      new Consumer[GenericContainer[_]]() {
        override def accept(arg: GenericContainer[_]): Unit = ()
      }
) {

  /**
   * Java Api
   */
  def getZooKeeperImage(): String = zooKeeperImage

  /**
   * Java Api
   */
  def getZooKeeperImageTag(): String = zooKeeperImageTag

  /**
   * Java Api
   */
  def getKafkaImage(): String = kafkaImage

  /**
   * Java Api
   */
  def getKafkaImageTag(): String = kafkaImageTag

  /**
   * Java Api
   */
  def getSchemaRegistryImage(): String = schemaRegistryImage

  /**
   * Java Api
   */
  def getSchemaRegistryImageTag(): String = schemaRegistryImageTag

  /**
   * Java Api
   */
  def getNumBrokers(): Int = numBrokers

  /**
   * Java Api
   */
  def getInternalTopicsReplicationFactor(): Int = internalTopicsReplicationFactor

  /**
   * Java Api
   */
  def getSchemaRegistry(): Boolean = useSchemaRegistry

  /**
   * Java Api
   */
  def getContainerLogging(): Boolean = containerLogging

  /**
   * Java Api
   */
  def getClusterStartTimeout(): Duration = clusterStartTimeout.toJava

  /**
   * Java Api
   */
  def getReadinessCheckTimeout(): Duration = readinessCheckTimeout.toJava

  /**
   * Sets the ZooKeeper image
   */
  def withZooKeeperImage(zooKeeperImage: String): KafkaTestkitTestcontainersSettings =
    copy(zooKeeperImage = zooKeeperImage)

  /**
   * Sets the ZooKeeper image tag
   */
  def withZooKeeperImageTag(zooKeeperImageTag: String): KafkaTestkitTestcontainersSettings =
    copy(zooKeeperImageTag = zooKeeperImageTag)

  /**
   * Sets the Kafka image
   */
  def withKafkaImage(kafkaImage: String): KafkaTestkitTestcontainersSettings =
    copy(kafkaImage = kafkaImage)

  /**
   * Sets the Kafka image tag
   */
  def withKafkaImageTag(kafkaImageTag: String): KafkaTestkitTestcontainersSettings =
    copy(kafkaImageTag = kafkaImageTag)

  /**
   * Sets the Schema Registry image
   */
  def withSchemaRegistryImage(schemaRegistryImage: String): KafkaTestkitTestcontainersSettings =
    copy(schemaRegistryImage = schemaRegistryImage)

  /**
   * Sets the Schema Registry image tag
   */
  def withSchemaRegistryImageTag(schemaRegistryImageTag: String): KafkaTestkitTestcontainersSettings =
    copy(schemaRegistryImageTag = schemaRegistryImageTag)

  /**
   * Replaces the default number of Kafka brokers
   */
  def withNumBrokers(numBrokers: Int): KafkaTestkitTestcontainersSettings =
    copy(numBrokers = numBrokers)

  /**
   * Replaces the default internal Kafka topics replication factor
   */
  def withInternalTopicsReplicationFactor(internalTopicsReplicationFactor: Int): KafkaTestkitTestcontainersSettings =
    copy(internalTopicsReplicationFactor = internalTopicsReplicationFactor)

  /**
   * Java Api
   *
   * Replaces the default Kafka testcontainers configuration logic
   */
  def withConfigureKafkaConsumer(
      configureKafkaConsumer: java.util.function.Consumer[java.util.Collection[AlpakkaKafkaContainer]]
  ): KafkaTestkitTestcontainersSettings = copy(configureKafkaConsumer = configureKafkaConsumer)

  /**
   * Replaces the default Kafka testcontainers configuration logic
   */
  def withConfigureKafka(configureKafka: Vector[AlpakkaKafkaContainer] => Unit): KafkaTestkitTestcontainersSettings =
    copy(configureKafka = configureKafka)

  /**
   * Replaces the default ZooKeeper testcontainers configuration logic
   */
  def withConfigureZooKeeper(configureZooKeeper: GenericContainer[_] => Unit): KafkaTestkitTestcontainersSettings =
    copy(configureZooKeeper = configureZooKeeper)

  /**
   * Java Api
   *
   * Replaces the default ZooKeeper testcontainers configuration logic
   */
  def withConfigureZooKeeperConsumer(
      configureZooKeeperConsumer: java.util.function.Consumer[GenericContainer[_]]
  ): KafkaTestkitTestcontainersSettings =
    copy(configureZooKeeperConsumer = configureZooKeeperConsumer)

  /**
   * Use Schema Registry container.
   */
  def withSchemaRegistry(useSchemaRegistry: Boolean): KafkaTestkitTestcontainersSettings =
    copy(useSchemaRegistry = useSchemaRegistry);

  /**
   * Stream container output to SLF4J logger(s).
   */
  def withContainerLogging(containerLogging: Boolean): KafkaTestkitTestcontainersSettings =
    copy(containerLogging = containerLogging)

  /**
   * Kafka cluster start up timeout
   */
  def withClusterStartTimeout(timeout: FiniteDuration): KafkaTestkitTestcontainersSettings =
    copy(clusterStartTimeout = timeout)

  /**
   * Java Api
   *
   * Kafka cluster start up timeout
   */
  def withClusterStartTimeout(timeout: Duration): KafkaTestkitTestcontainersSettings =
    copy(clusterStartTimeout = timeout.toScala)

  /**
   * Kafka cluster readiness check timeout
   */
  def withReadinessCheckTimeout(timeout: FiniteDuration): KafkaTestkitTestcontainersSettings =
    copy(readinessCheckTimeout = timeout)

  /**
   * Java Api
   *
   * Kafka cluster readiness check timeout
   */
  def withReadinessCheckTimeout(timeout: Duration): KafkaTestkitTestcontainersSettings =
    copy(readinessCheckTimeout = timeout.toScala)

  private def copy(
      zooKeeperImage: String = zooKeeperImage,
      zooKeeperImageTag: String = zooKeeperImageTag,
      kafkaImage: String = kafkaImage,
      kafkaImageTag: String = kafkaImageTag,
      schemaRegistryImage: String = schemaRegistryImage,
      schemaRegistryImageTag: String = schemaRegistryImageTag,
      numBrokers: Int = numBrokers,
      internalTopicsReplicationFactor: Int = internalTopicsReplicationFactor,
      useSchemaRegistry: Boolean = useSchemaRegistry,
      containerLogging: Boolean = containerLogging,
      clusterStartTimeout: FiniteDuration = clusterStartTimeout,
      readinessCheckTimeout: FiniteDuration = readinessCheckTimeout,
      configureKafka: Vector[AlpakkaKafkaContainer] => Unit = configureKafka,
      configureKafkaConsumer: java.util.function.Consumer[java.util.Collection[AlpakkaKafkaContainer]] =
        configureKafkaConsumer,
      configureZooKeeper: GenericContainer[_] => Unit = configureZooKeeper,
      configureZooKeeperConsumer: java.util.function.Consumer[GenericContainer[_]] = configureZooKeeperConsumer
  ): KafkaTestkitTestcontainersSettings =
    new KafkaTestkitTestcontainersSettings(zooKeeperImage,
                                           zooKeeperImageTag,
                                           kafkaImage,
                                           kafkaImageTag,
                                           schemaRegistryImage,
                                           schemaRegistryImageTag,
                                           numBrokers,
                                           internalTopicsReplicationFactor,
                                           useSchemaRegistry,
                                           containerLogging,
                                           clusterStartTimeout,
                                           readinessCheckTimeout,
                                           configureKafka,
                                           configureKafkaConsumer,
                                           configureZooKeeper,
                                           configureZooKeeperConsumer)

  override def toString: String =
    "KafkaTestkitTestcontainersSettings(" +
    s"zooKeeperImage=$zooKeeperImage," +
    s"zooKeeperImageTag=$zooKeeperImageTag," +
    s"kafkaImage=$kafkaImage," +
    s"kafkaImageTag=$kafkaImageTag," +
    s"schemaRegistryImage=$schemaRegistryImage," +
    s"schemaRegistryImageTag=$schemaRegistryImageTag," +
    s"numBrokers=$numBrokers," +
    s"internalTopicsReplicationFactor=$internalTopicsReplicationFactor," +
    s"useSchemaRegistry=$useSchemaRegistry," +
    s"containerLogging=$containerLogging, +" +
    s"clusterStartTimeout=${clusterStartTimeout.toCoarsest}," +
    s"readinessCheckTimeout=${readinessCheckTimeout.toCoarsest})"
}

object KafkaTestkitTestcontainersSettings {
  final val ConfigPath = "akka.kafka.testkit.testcontainers"

  /**
   * Create testkit testcontainers settings from ActorSystem settings.
   */
  def apply(system: ActorSystem): KafkaTestkitTestcontainersSettings =
    KafkaTestkitTestcontainersSettings(system.settings.config.getConfig(ConfigPath))

  /**
   * Java Api
   *
   * Create testkit testcontainers settings from ActorSystem settings.
   */
  def create(system: ActorSystem): KafkaTestkitTestcontainersSettings = KafkaTestkitTestcontainersSettings(system)

  /**
   * Create testkit testcontainres settings from a Config.
   */
  def apply(config: Config): KafkaTestkitTestcontainersSettings = {
    val zooKeeperImage = config.getString("zookeeper-image")
    val zooKeeperImageTag = config.getString("zookeeper-image-tag")
    val kafkaImage = config.getString("kafka-image")
    val kafkaImageTag = config.getString("kafka-image-tag")
    val schemaRegistryImage = config.getString("schema-registry-image")
    val schemaRegistryImageTag = config.getString("schema-registry-image-tag")
    val numBrokers = config.getInt("num-brokers")
    val internalTopicsReplicationFactor = config.getInt("internal-topics-replication-factor")
    val useSchemaRegistry = config.getBoolean("use-schema-registry")
    val containerLogging = config.getBoolean("container-logging")
    val clusterStartTimeout = config.getDuration("cluster-start-timeout").toScala
    val readinessCheckTimeout = config.getDuration("readiness-check-timeout").toScala

    new KafkaTestkitTestcontainersSettings(zooKeeperImage,
                                           zooKeeperImageTag,
                                           kafkaImage,
                                           kafkaImageTag,
                                           schemaRegistryImage,
                                           schemaRegistryImageTag,
                                           numBrokers,
                                           internalTopicsReplicationFactor,
                                           useSchemaRegistry,
                                           containerLogging,
                                           clusterStartTimeout,
                                           readinessCheckTimeout)
  }

  /**
   * Java Api
   *
   * Create testkit settings from a Config.
   */
  def create(config: Config): KafkaTestkitTestcontainersSettings = KafkaTestkitTestcontainersSettings(config)
}
