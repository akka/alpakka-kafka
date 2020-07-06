/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit

import java.util.function.Consumer

import akka.actor.ActorSystem
import akka.kafka.testkit.internal.AlpakkaKafkaContainer
import com.typesafe.config.Config
import org.testcontainers.containers.GenericContainer

final class KafkaTestkitTestcontainersSettings private (
    val confluentPlatformVersion: String,
    val numBrokers: Int,
    val internalTopicsReplicationFactor: Int,
    val useSchemaRegistry: Boolean,
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
  def getConfluentPlatformVersion(): String = confluentPlatformVersion

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
   * Sets the Confluent Platform Version
   */
  def withConfluentPlatformVersion(confluentPlatformVersion: String): KafkaTestkitTestcontainersSettings =
    copy(confluentPlatformVersion = confluentPlatformVersion)

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

  private def copy(
      confluentPlatformVersion: String = confluentPlatformVersion,
      numBrokers: Int = numBrokers,
      internalTopicsReplicationFactor: Int = internalTopicsReplicationFactor,
      useSchemaRegistry: Boolean = useSchemaRegistry,
      configureKafka: Vector[AlpakkaKafkaContainer] => Unit = configureKafka,
      configureKafkaConsumer: java.util.function.Consumer[java.util.Collection[AlpakkaKafkaContainer]] =
        configureKafkaConsumer,
      configureZooKeeper: GenericContainer[_] => Unit = configureZooKeeper,
      configureZooKeeperConsumer: java.util.function.Consumer[GenericContainer[_]] = configureZooKeeperConsumer
  ): KafkaTestkitTestcontainersSettings =
    new KafkaTestkitTestcontainersSettings(confluentPlatformVersion,
                                           numBrokers,
                                           internalTopicsReplicationFactor,
                                           useSchemaRegistry,
                                           configureKafka,
                                           configureKafkaConsumer,
                                           configureZooKeeper,
                                           configureZooKeeperConsumer)

  override def toString: String =
    "KafkaTestkitTestcontainersSettings(" +
    s"confluentPlatformVersion=$confluentPlatformVersion," +
    s"numBrokers=$numBrokers," +
    s"internalTopicsReplicationFactor=$internalTopicsReplicationFactor," +
    s"useSchemaRegistry=$useSchemaRegistry)"
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
    val confluentPlatformVersion = config.getString("confluent-platform-version")
    val numBrokers = config.getInt("num-brokers")
    val internalTopicsReplicationFactor = config.getInt("internal-topics-replication-factor")
    val useSchemaRegistry = config.getBoolean("use-schema-registry")

    new KafkaTestkitTestcontainersSettings(confluentPlatformVersion,
                                           numBrokers,
                                           internalTopicsReplicationFactor,
                                           useSchemaRegistry)
  }

  /**
   * Java Api
   *
   * Create testkit settings from a Config.
   */
  def create(config: Config): KafkaTestkitTestcontainersSettings = KafkaTestkitTestcontainersSettings(config)
}
