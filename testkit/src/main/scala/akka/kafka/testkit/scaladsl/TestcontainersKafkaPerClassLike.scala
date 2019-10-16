/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import akka.kafka.testkit.internal.TestcontainersKafkaHelper
import org.testcontainers.containers.KafkaContainer


/**
 * Uses [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka broker in a Docker container.
 * The Testcontainers dependency has to be added explicitly.
 */
trait TestcontainersKafkaPerClassLike extends KafkaSpec {

  private var kafkaContainer: KafkaContainer = _
  private var kafkaBootstrapServersInternal: String = _
  private var kafkaPortInternal: Int = -1

  private def requireStarted(): Unit =
    require(kafkaPortInternal != -1, "Testcontainers Kafka hasn't been started via `setUp`")

  override def kafkaPort: Int = {
    requireStarted()
    kafkaPortInternal
  }

  def bootstrapServers: String = {
    requireStarted()
    kafkaBootstrapServersInternal
  }

  def startKafka(confluentPlatformVersion: String): String = {
    if (kafkaPortInternal == -1) {
      val kafkaContainer = new KafkaContainer(confluentPlatformVersion)
      configureKafka(kafkaContainer)
      kafkaContainer.start()
      kafkaBootstrapServersInternal = kafkaContainer.getBootstrapServers
      kafkaPortInternal =
        kafkaBootstrapServersInternal.substring(kafkaBootstrapServersInternal.lastIndexOf(":") + 1).toInt
    }
    kafkaBootstrapServersInternal
  }

  def stopKafka(): Unit =
    if (kafkaPortInternal != -1) {
      kafkaContainer.stop()
      kafkaPortInternal = -1
      kafkaContainer = null
    }

  /**
   * Override this to select a different Kafka version be choosing the desired version of Confluent Platform:
   * [[https://hub.docker.com/r/confluentinc/cp-kafka/tags Available Docker images]],
   * [[https://docs.confluent.io/current/installation/versions-interoperability.html Kafka versions in Confluent Platform]]
   */
  def confluentPlatformVersion: String = TestcontainersKafkaHelper.ConfluentPlatformVersionDefault

  /**
   * Override this to configure the Kafka container before it is started.
   */
  def configureKafka(kafkaContainer: KafkaContainer): Unit = ()

  override def setUp(): Unit = {
    startKafka(confluentPlatformVersion)
    super.setUp()
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
    stopKafka()
  }

}
