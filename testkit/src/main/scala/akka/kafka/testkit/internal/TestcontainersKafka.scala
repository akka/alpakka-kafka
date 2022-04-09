/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal

import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.KafkaSpec
import akka.util.JavaDurationConverters._
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

object TestcontainersKafka {
  trait Spec extends KafkaSpec {
    private var kafkaBootstrapServersInternal: String = _
    private var kafkaPortInternal: Int = -1

    private def requireStarted(): Unit =
      require(kafkaPortInternal != -1, "Testcontainers Kafka hasn't been started via `setUp`")

    private var cluster: KafkaContainerCluster = {
      requireStarted()
      null
    }

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

    def getSchemaRegistryUrl: String =
      cluster.getSchemaRegistry.asScala
        .map(_.getSchemaRegistryUrl)
        .getOrElse(
          throw new RuntimeException("Did you enable schema registry in your KafkaTestkitTestcontainersSettings?")
        )

    def startCluster(): String = startCluster(testcontainersSettings)

    def startCluster(settings: KafkaTestkitTestcontainersSettings): String = {
      import settings._
      // check if already initialized
      if (kafkaPortInternal == -1) {
        cluster = new KafkaContainerCluster(
          DockerImageName.parse(settings.zooKeeperImage).withTag(settings.zooKeeperImageTag),
          DockerImageName.parse(settings.kafkaImage).withTag(settings.kafkaImageTag),
          DockerImageName.parse(settings.schemaRegistryImage).withTag(settings.schemaRegistryImageTag),
          numBrokers,
          internalTopicsReplicationFactor,
          settings.useSchemaRegistry,
          settings.containerLogging,
          settings.clusterStartTimeout.asJava,
          settings.readinessCheckTimeout.asJava
        )
        configureKafka(brokerContainers)
        configureKafkaConsumer.accept(brokerContainers.asJavaCollection)
        configureZooKeeper(zookeeperContainer)
        configureZooKeeperConsumer.accept(zookeeperContainer)
        schemaRegistryContainer match {
          case Some(container) => configureSchemaRegistry(container)
          case _ =>
        }
        log.info("Starting Kafka cluster with settings: {}", settings)
        cluster.start()
        kafkaBootstrapServersInternal = cluster.getBootstrapServers
        kafkaPortInternal =
          kafkaBootstrapServersInternal.substring(kafkaBootstrapServersInternal.lastIndexOf(":") + 1).toInt
      }
      kafkaBootstrapServersInternal
    }

    def stopCluster(): Unit =
      if (kafkaPortInternal != -1) {
        cluster.stop()
        kafkaPortInternal = -1
        cluster = null
      }

    def startKafka(): Unit = cluster.startKafka()

    def stopKafka(): Unit = cluster.stopKafka()

    def schemaRegistryUrl: String =
      schemaRegistryContainer
        .map(_.getSchemaRegistryUrl)
        .getOrElse(
          throw new RuntimeException("Did you enable schema registry in your KafkaTestkitTestcontainersSettings?")
        )
  }

  // the test base type used for Singleton cannot reference ScalaTest types so that it's compatible with JUnit-only test projects
  // see https://github.com/akka/alpakka-kafka/issues/1327
  private class SpecBase extends KafkaSpec(-1) with Spec

  val Singleton: Spec = new SpecBase
}
