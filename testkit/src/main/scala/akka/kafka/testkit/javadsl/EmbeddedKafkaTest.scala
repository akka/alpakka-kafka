/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

abstract class EmbeddedKafkaTest extends KafkaTest {}

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
