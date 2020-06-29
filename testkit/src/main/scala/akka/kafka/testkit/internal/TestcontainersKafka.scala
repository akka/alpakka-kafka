/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal

import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.{KafkaSpec, ScalatestKafkaSpec}
import org.testcontainers.containers.GenericContainer

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

object TestcontainersKafka {
  trait Spec extends KafkaSpec {
    private var cluster: KafkaContainerCluster = _
    private var kafkaBootstrapServersInternal: String = _
    private var kafkaPortInternal: Int = -1

    private def requireStarted(): Unit =
      require(kafkaPortInternal != -1, "Testcontainers Kafka hasn't been started via `setUp`")

    /**
     * Override this to select a different Kafka version be choosing the desired version of Confluent Platform:
     * [[https://hub.docker.com/r/confluentinc/cp-kafka/tags Available Docker images]],
     * [[https://docs.confluent.io/current/installation/versions-interoperability.html Kafka versions in Confluent Platform]]
     *
     * Deprecated: set Confluent Platform version in [[KafkaTestkitTestcontainersSettings]]
     */
    @deprecated("Use testcontainersSettings instead.", "2.0.0")
    def confluentPlatformVersion: String = KafkaContainerCluster.CONFLUENT_PLATFORM_VERSION

    /**
     * Override this to change default settings for starting the Kafka testcontainers cluster.
     */
    val testcontainersSettings: KafkaTestkitTestcontainersSettings = KafkaTestkitTestcontainersSettings(system)

    override def kafkaPort: Int = {
      requireStarted()
      // it's possible there's more than 1 broker, provide the port of the first broker
      kafkaPortInternal
    }

    def bootstrapServers: String = {
      requireStarted()
      kafkaBootstrapServersInternal
    }

    def brokerContainers: Vector[AlpakkaKafkaContainer] = cluster.getBrokers.asScala.toVector

    def zookeeperContainer: GenericContainer[_] = cluster.getZooKeeper

    def schemaRegistryContainer: Option[SchemaRegistryContainer] = cluster.getSchemaRegistry.asScala

    /**
     * Deprecated: Starts Kafka, but does not override with provided settings.
     */
    @deprecated("Use startKafkaWithSettings instead", "2.0.3")
    def startKafka(settings: KafkaTestkitTestcontainersSettings): String = {
      startKafkaWithSettings(testcontainersSettings)
    }

    def startKafka(): String = {
      startKafkaWithSettings(testcontainersSettings)
    }

    def startKafkaWithSettings(settings: KafkaTestkitTestcontainersSettings): String = {
      import settings._
      // check if already initialized
      if (kafkaPortInternal == -1) {
        cluster = new KafkaContainerCluster(settings.confluentPlatformVersion,
                                            numBrokers,
                                            internalTopicsReplicationFactor,
                                            settings.useSchemaRegistry)
        configureKafka(brokerContainers)
        configureKafkaConsumer.accept(brokerContainers.asJavaCollection)
        configureZooKeeper(zookeeperContainer)
        configureZooKeeperConsumer.accept(zookeeperContainer)
        log.info("Starting Kafka cluster with settings: {}", settings)
        cluster.start()
        kafkaBootstrapServersInternal = cluster.getBootstrapServers
        kafkaPortInternal =
          kafkaBootstrapServersInternal.substring(kafkaBootstrapServersInternal.lastIndexOf(":") + 1).toInt
      }
      kafkaBootstrapServersInternal
    }

    def stopKafka(): Unit =
      if (kafkaPortInternal != -1) {
        cluster.stop()
        kafkaPortInternal = -1
        cluster = null
      }

    def schemaRegistryUrl: String =
      schemaRegistryContainer
        .map(_.getSchemaRegistryUrl)
        .getOrElse(
          throw new RuntimeException("Did you enable schema registry in your KafkaTestkitTestcontainersSettings?")
        )
  }

  private class SpecBase extends ScalatestKafkaSpec(-1) with Spec

  val Singleton: Spec = new SpecBase
}
