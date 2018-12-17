/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.junit.jupiter.api.{AfterAll, BeforeAll}

/**
 * JUnit 5 aka Jupiter base-class with some convenience for creating an embedded Kafka broker
 * before running the tests.
 * Extending classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)` to create
 * a single instance of the test class with `@BeforeAll` and `@AfterAll` annotated methods called
 * by the test framework.
 */
abstract class EmbeddedKafkaTest extends KafkaTest {
  import EmbeddedKafkaJunit4Test._

  def kafkaPort: Int
  def replicationFactor = 1

  @BeforeAll def setupEmbeddedKafka() = startEmbeddedKafka(kafkaPort, replicationFactor)

  @AfterAll def cleanUpEmbeddedKafka() =
    stopEmbeddedKafka()
}

object EmbeddedKafkaTest {
  private def embeddedKafkaConfig(kafkaPort: Int, zooKeeperPort: Int, replicationFactor: Int) =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> s"$replicationFactor"
                        ))

  def startEmbeddedKafka(kafkaPort: Int, replicationFactor: Int): Unit =
    EmbeddedKafka.start()(embeddedKafkaConfig(kafkaPort, kafkaPort + 1, replicationFactor))

  def stopEmbeddedKafka(): Unit =
    EmbeddedKafka.stop()
}
