/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import akka.kafka.testkit.internal.TestcontainersKafkaHelper

/**
 * Uses [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka broker in a Docker container.
 * The Testcontainers dependency has to be added explicitly.
 */
trait TestcontainersKafkaLike extends KafkaSpec {
  import akka.kafka.testkit.internal.TestcontainersKafkaHelper._

  /**
   * Override this to select a different Kafka version be choosing the desired version of Confluent Platform:
   * [[https://hub.docker.com/r/confluentinc/cp-kafka/tags Available Docker images]],
   * [[https://docs.confluent.io/current/installation/versions-interoperability.html Kafka versions in Confluent Platform]]
   */
  def confluentPlatformVersion: String = ConfluentPlatformVersionDefault

  override def kafkaPort: Int = TestcontainersKafkaHelper.kafkaPort

  override def bootstrapServers: String = TestcontainersKafkaHelper.bootstrapServers

  override def setUp(): Unit = {
    TestcontainersKafkaHelper.startKafka(confluentPlatformVersion)
    super.setUp()
  }

  def stopKafka(): Unit = TestcontainersKafkaHelper.stopKafka()
}
