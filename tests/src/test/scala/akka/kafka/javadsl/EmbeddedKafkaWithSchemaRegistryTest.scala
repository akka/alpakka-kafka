/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl
import akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test
import net.manub.embeddedkafka.schemaregistry.{
  EmbeddedKWithSR,
  EmbeddedKafkaConfigWithSchemaRegistryImpl,
  EmbeddedKafkaWithSchemaRegistry
}
import org.junit.{After, Before}

abstract class EmbeddedKafkaWithSchemaRegistryTest extends EmbeddedKafkaJunit4Test {
  import EmbeddedKafkaWithSchemaRegistryTest._

  @Before override def setupEmbeddedKafka() =
    startEmbeddedKafka(kafkaPort, replicationFactor)

  @After override def cleanUpEmbeddedKafka(): Unit =
    stopEmbeddedKafka()
}

object EmbeddedKafkaWithSchemaRegistryTest {

  /**
   * Workaround for https://github.com/manub/scalatest-embedded-kafka/issues/166
   * Keeping track of all embedded servers, so we can shut the down later
   */
  private var embeddedServer: EmbeddedKWithSR = _

  def schemaRegistryPort(kafkaPort: Int) =
    kafkaPort + 2

  private def embeddedKafkaConfig(kafkaPort: Int, zooKeeperPort: Int, schemaRegistryPort: Int, replicationFactor: Int) =
    EmbeddedKafkaConfigWithSchemaRegistryImpl(
      kafkaPort,
      zooKeeperPort,
      schemaRegistryPort,
      Map(
        "offsets.topic.replication.factor" -> s"$replicationFactor",
        "zookeeper.connection.timeout.ms" -> "20000"
      ),
      Map.empty,
      Map.empty
    )

  def startEmbeddedKafka(kafkaPort: Int, replicationFactor: Int): Unit =
    embeddedServer = EmbeddedKafkaWithSchemaRegistry.start()(
      embeddedKafkaConfig(kafkaPort, kafkaPort + 1, schemaRegistryPort(kafkaPort), replicationFactor)
    )

  def stopEmbeddedKafka(): Unit =
    embeddedServer.stop(clearLogs = true)
}
