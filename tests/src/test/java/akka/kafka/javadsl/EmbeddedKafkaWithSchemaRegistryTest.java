/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl;

import akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test;
import akka.kafka.testkit.javadsl.KafkaTest;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKWithSR;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfigWithSchemaRegistry;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfigWithSchemaRegistry$;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaWithSchemaRegistry$;
import org.junit.After;
import org.junit.Before;
import scala.collection.immutable.HashMap$;

public abstract class EmbeddedKafkaWithSchemaRegistryTest extends KafkaTest {

  /**
   * Workaround for https://github.com/manub/scalatest-embedded-kafka/issues/166 Keeping track of
   * all embedded servers, so we can shut the down later
   */
  private static EmbeddedKWithSR embeddedServer;

  private static EmbeddedKafkaConfigWithSchemaRegistry embeddedKafkaConfig(
      int kafkaPort, int zookeeperPort, int schemaRegistryPort, int replicationFactor) {
    return EmbeddedKafkaConfigWithSchemaRegistry$.MODULE$.apply(
        kafkaPort,
        zookeeperPort,
        schemaRegistryPort,
        createReplicationFactorBrokerProps(replicationFactor)
            .updated("zookeeper.connection.timeout.ms", "20000"),
        HashMap$.MODULE$.empty(),
        HashMap$.MODULE$.empty());
  }

  public static int schemaRegistryPort(int kafkaPort) {
    return kafkaPort + 2;
  }

  protected static void startEmbeddedKafka(int kafkaPort, int replicationFactor) {
    embeddedServer =
        EmbeddedKafkaWithSchemaRegistry$.MODULE$.start(
            embeddedKafkaConfig(
                kafkaPort, kafkaPort + 1, schemaRegistryPort(kafkaPort), replicationFactor));
  }

  protected static void stopEmbeddedKafka() {
    EmbeddedKafka$.MODULE$.stop();
  }

  public abstract int kafkaPort();

  public int replicationFactor() {
    return 1;
  }

  @Before
  public void setupEmbeddedKafka() {
    EmbeddedKafkaWithSchemaRegistryTest.startEmbeddedKafka(kafkaPort(), replicationFactor());
    setUpAdminClient();
  }

  @After
  public void cleanUpEmbeddedKafka() {
    cleanUpAdminClient();
    EmbeddedKafkaWithSchemaRegistryTest.embeddedServer.stop(true);
  }
}
