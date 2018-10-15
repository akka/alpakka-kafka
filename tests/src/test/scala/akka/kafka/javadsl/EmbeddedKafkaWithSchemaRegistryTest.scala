/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafkaConfigWithSchemaRegistryImpl, EmbeddedKafkaWithSchemaRegistry}

abstract class EmbeddedKafkaWithSchemaRegistryTest extends KafkaTest {}

object EmbeddedKafkaWithSchemaRegistryTest {

  private def embeddedKafkaConfig(kafkaPort: Int, zooKeeperPort: Int, schemaRegistryPort: Int, replicationFactor: Int) =
    EmbeddedKafkaConfigWithSchemaRegistryImpl(kafkaPort,
                                              zooKeeperPort,
                                              schemaRegistryPort,
                                              Map(
                                                "offsets.topic.replication.factor" -> s"$replicationFactor"
                                              ),
                                              Map.empty,
                                              Map.empty)

  def startEmbeddedKafka(kafkaPort: Int, zookeeperPort: Int, schemaRegistryPort: Int, replicationFactor: Int): Unit =
    EmbeddedKafkaWithSchemaRegistry.start()(
      embeddedKafkaConfig(kafkaPort, zookeeperPort, schemaRegistryPort, replicationFactor)
    )

  def stopEmbeddedKafka(): Unit =
    EmbeddedKafkaWithSchemaRegistry.stop()
}
