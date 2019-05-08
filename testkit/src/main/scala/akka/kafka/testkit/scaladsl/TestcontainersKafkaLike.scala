/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import org.testcontainers.containers.KafkaContainer

/**
 * Uses [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka broker in a Docker container.
 * The Testcontainers dependency has to be added explicitly.
 */
trait TestcontainersKafkaLike extends KafkaSpec {
  import TestcontainersKafkaLike._

  /**
   * Override this to select a different Kafka version be choosing the desired version of Confluent Platform:
   * [[https://hub.docker.com/r/confluentinc/cp-kafka/tags Available Docker images]],
   * [[https://docs.confluent.io/current/installation/versions-interoperability.html Kafka versions in Confluent Platform]]
   */
  def confluentPlatformVersion: String = "5.1.2" // contains Kafka 2.1.x

  override def kafkaPort: Int = {
    requireStarted()
    kafkaPortInternal
  }

  override def bootstrapServers: String = {
    requireStarted()
    kafkaBootstrapServersInternal
  }

  override def setUp(): Unit = {
    if (kafkaPortInternal == -1) {
      val kafkaContainer = new KafkaContainer(confluentPlatformVersion)
      kafkaContainer.start()
      kafkaBootstrapServersInternal = kafkaContainer.getBootstrapServers
      kafkaPortInternal =
        kafkaBootstrapServersInternal.substring(kafkaBootstrapServersInternal.lastIndexOf(":") + 1).toInt
    }
    super.setUp()
  }
}

private object TestcontainersKafkaLike {

  private var kafkaBootstrapServersInternal: String = _
  private var kafkaPortInternal: Int = -1

  private def requireStarted(): Unit =
    require(kafkaPortInternal != -1, "Testcontainers Kafka hasn't been started via `setUp`")

}
