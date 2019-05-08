/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.testkit.javadsl.KafkaJunit4Test;
import akka.stream.Materializer;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKWithSR;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfig;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfig$;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$;
import org.junit.After;
import org.junit.Before;
import scala.collection.immutable.HashMap$;

public abstract class EmbeddedKafkaWithSchemaRegistryTest extends KafkaJunit4Test {

  /**
   * Workaround for https://github.com/manub/scalatest-embedded-kafka/issues/166 Keeping track of
   * all embedded servers, so we can shut the down later
   */
  private static EmbeddedKWithSR embeddedServer;

  protected final int kafkaPort;
  protected final int replicationFactor;
  protected final int schemaRegistryPort;
  protected final String schemaRegistryUrl;

  public EmbeddedKafkaWithSchemaRegistryTest(
      ActorSystem system,
      Materializer materializer,
      int kafkaPort,
      int replicationFactor,
      int schemaRegistryPort) {
    super(system, materializer, "localhost:" + kafkaPort);
    this.kafkaPort = kafkaPort;
    this.replicationFactor = replicationFactor;
    this.schemaRegistryPort = schemaRegistryPort;
    this.schemaRegistryUrl = "http://localhost:" + schemaRegistryPort;
  }

  private static EmbeddedKafkaConfig embeddedKafkaConfig(
      int kafkaPort, int zookeeperPort, int schemaRegistryPort, int replicationFactor) {
    return EmbeddedKafkaConfig$.MODULE$.apply(
        kafkaPort,
        zookeeperPort,
        schemaRegistryPort,
        createReplicationFactorBrokerProps(replicationFactor)
            .updated("zookeeper.connection.timeout.ms", "20000"),
        HashMap$.MODULE$.empty(),
        HashMap$.MODULE$.empty());
  }

  protected static void startEmbeddedKafka(
      int kafkaPort, int replicationFactor, int schemaRegistryPort) {
    embeddedServer =
        EmbeddedKafka$.MODULE$.start(
            embeddedKafkaConfig(kafkaPort, kafkaPort + 1, schemaRegistryPort, replicationFactor));
  }

  @Before
  public void setUpEmbeddedKafka() {
    EmbeddedKafkaWithSchemaRegistryTest.startEmbeddedKafka(
        kafkaPort, replicationFactor, schemaRegistryPort);
    setUpAdminClient();
  }

  @After
  public void cleanUpEmbeddedKafka() {
    cleanUpAdminClient();
    EmbeddedKafkaWithSchemaRegistryTest.embeddedServer.stop(true);
  }
}
