/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.testcontainers.containers.{GenericContainer, KafkaContainer}

final class KafkaTestkitTestcontainersSettings private (
    val confluentPlatformVersion: String,
    val numBrokers: Int,
    val internalTopicsReplicationFactor: Int,
    val configureKafka: Vector[KafkaContainer] => Unit = _ => (),
    val configureKafkaJava: java.util.function.Consumer[java.util.Collection[KafkaContainer]] = _ => (),
    val configureZooKeeper: GenericContainer[_] => Unit = _ => ()
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
  def withConfigureKafkaJava(
      configureKafkaJava: java.util.function.Consumer[java.util.Collection[KafkaContainer]]
  ): KafkaTestkitTestcontainersSettings = copy(configureKafkaJava = configureKafkaJava)

  /**
   * Replaces the default Kafka testcontainers configuration logic
   */
  def withConfigureKafka(configureKafka: Vector[KafkaContainer] => Unit): KafkaTestkitTestcontainersSettings =
    copy(configureKafka = configureKafka)

  /**
   * Replaces the default ZooKeeper testcontainers configuration logic
   */
  def withConfigureZooKeeper(configureZooKeeper: GenericContainer[_] => Unit): KafkaTestkitTestcontainersSettings =
    copy(configureZooKeeper = configureZooKeeper)

  private def copy(
      confluentPlatformVersion: String = confluentPlatformVersion,
      numBrokers: Int = numBrokers,
      internalTopicsReplicationFactor: Int = internalTopicsReplicationFactor,
      configureKafka: Vector[KafkaContainer] => Unit = configureKafka,
      configureKafkaJava: java.util.function.Consumer[java.util.Collection[KafkaContainer]] = configureKafkaJava,
      configureZooKeeper: GenericContainer[_] => Unit = configureZooKeeper
  ): KafkaTestkitTestcontainersSettings =
    new KafkaTestkitTestcontainersSettings(confluentPlatformVersion,
                                           numBrokers,
                                           internalTopicsReplicationFactor,
                                           configureKafka,
                                           configureKafkaJava,
                                           configureZooKeeper)

  override def toString: String =
    "KafkaTestkitTestcontainersSettings(" +
    s"confluentPlatformVersion=$confluentPlatformVersion," +
    s"numBrokers=$numBrokers," +
    s"internalTopicsReplicationFactor=$internalTopicsReplicationFactor)"
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

    new KafkaTestkitTestcontainersSettings(confluentPlatformVersion, numBrokers, internalTopicsReplicationFactor)
  }

  /**
   * Java Api
   *
   * Create testkit settings from a Config.
   */
  def create(config: Config): KafkaTestkitTestcontainersSettings = KafkaTestkitTestcontainersSettings(config)
}
