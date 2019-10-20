/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.internal

import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.{KafkaSpec, ScalatestKafkaSpec}
import org.testcontainers.containers.GenericContainer
//import org.testcontainers.containers.output.Slf4jLogConsumer

import scala.collection.JavaConverters._

object TestcontainersKafka {
  val ConfluentPlatformVersion: String = KafkaContainer.CONFLUENT_PLATFORM_VERSION

  trait Spec extends KafkaSpec {
    //private val logConsumer = new Slf4jLogConsumer(log)
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
    @deprecated("Use testcontainersSettings instead.", "1.1.1")
    def confluentPlatformVersion: String = ConfluentPlatformVersion

    /**
     * Override this to change default settings for starting the Kafka testcontainers cluster.
     */
    val testcontainersSettings: KafkaTestkitTestcontainersSettings = KafkaTestkitTestcontainersSettings(system)

    override def kafkaPort: Int = {
      requireStarted()
      // it's possible there's more than 1 broker, provide the port of the first broker
      testcontainersSettings.startPort
    }

    def bootstrapServers: String = {
      requireStarted()
      kafkaBootstrapServersInternal
    }

    def brokerContainers: Vector[GenericContainer[_]] = cluster.getBrokers.asScala.toVector

    def zookeeperContainer: GenericContainer[_] = cluster.getZooKeeper

    def startKafka(settings: KafkaTestkitTestcontainersSettings): String = {
      val settings = testcontainersSettings
      import settings._
      // check if already initialized
      if (kafkaPortInternal == -1) {
        cluster = new KafkaContainerCluster(settings.confluentPlatformVersion,
                                            numBrokers,
                                            internalTopicsReplicationFactor,
                                            startPort)
        configureKafka(brokerContainers)
        configureZooKeeper(zookeeperContainer)
        log.info("Starting Kafka cluster with settings: {}", settings)
        cluster.startAll()
        // TODO: this form of logging doesn't seem to capture everything, do we need to initialize logging config in confluent containers?
        //logContainers()
        kafkaBootstrapServersInternal = cluster.getBootstrapServers
        kafkaPortInternal =
          kafkaBootstrapServersInternal.substring(kafkaBootstrapServersInternal.lastIndexOf(":") + 1).toInt
      }
      kafkaBootstrapServersInternal
    }

    def stopKafka(): Unit =
      if (kafkaPortInternal != -1) {
        cluster.stopAll()
        kafkaPortInternal = -1
        cluster = null
      }

//    private def logContainers(): Unit = {
//      brokerContainers.foreach(_.followOutput(logConsumer))
//      zookeeperContainer.followOutput(logConsumer)
//    }
  }

  private class SpecBase extends ScalatestKafkaSpec(-1) with Spec

  val Singleton: Spec = new SpecBase
}
