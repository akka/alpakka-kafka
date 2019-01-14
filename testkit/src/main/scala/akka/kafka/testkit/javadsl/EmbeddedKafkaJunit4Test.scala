/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.junit.{After, Before}

abstract class EmbeddedKafkaJunit4Test extends KafkaJunit4Test {
  import EmbeddedKafkaJunit4Test._

  def kafkaPort: Int
  def replicationFactor = 1

  @Before def setupEmbeddedKafka() = startEmbeddedKafka(kafkaPort, replicationFactor)

  @After def cleanUpEmbeddedKafka() =
    stopEmbeddedKafka()
}

object EmbeddedKafkaJunit4Test {
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
