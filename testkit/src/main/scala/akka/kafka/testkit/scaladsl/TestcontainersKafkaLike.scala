/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.internal.{AlpakkaKafkaContainer, SchemaRegistryContainer, TestcontainersKafka}
import org.testcontainers.containers.GenericContainer

/**
 * Uses [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka cluster in a Docker container.
 * This trait will start Kafka only once per test session.  To create a Kafka cluster per test class see
 * [[TestcontainersKafkaPerClassLike]].
 *
 * The Testcontainers dependency has to be added explicitly.
 */
trait TestcontainersKafkaLike extends TestcontainersKafka.Spec {
  override def kafkaPort: Int = TestcontainersKafka.Singleton.kafkaPort
  override def bootstrapServers: String = TestcontainersKafka.Singleton.bootstrapServers
  override def brokerContainers: Vector[AlpakkaKafkaContainer] = TestcontainersKafka.Singleton.brokerContainers
  override def zookeeperContainer: GenericContainer[_] = TestcontainersKafka.Singleton.zookeeperContainer
  override def schemaRegistryContainer: Option[SchemaRegistryContainer] =
    TestcontainersKafka.Singleton.schemaRegistryContainer
  override def schemaRegistryUrl: String = TestcontainersKafka.Singleton.schemaRegistryUrl
  override def startKafkaWithSettings(overrideSettings: KafkaTestkitTestcontainersSettings): String =
    TestcontainersKafka.Singleton.startKafkaWithSettings(overrideSettings)
  override def stopKafka(): Unit = TestcontainersKafka.Singleton.stopKafka()

  override def setUp(): Unit = {
    startKafkaWithSettings(testcontainersSettings)
    super.setUp()
  }

  override def cleanUp(): Unit = {
    // do nothing to keep everything running.  testcontainers runs as a daemon and will shut all containers down
    // when the sbt session terminates.
  }
}
