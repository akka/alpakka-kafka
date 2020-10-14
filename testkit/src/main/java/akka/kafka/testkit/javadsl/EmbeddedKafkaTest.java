/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import net.manub.embeddedkafka.EmbeddedKafkaConfig$;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import scala.collection.immutable.HashMap$;

/**
 * JUnit 5 aka Jupiter base-class with some convenience for creating an embedded Kafka broker before
 * running the tests. Extending classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)`
 * to create a single instance of the test class with `@BeforeAll` and `@AfterAll` annotated methods
 * called by the test framework.
 *
 * @deprecated Use testcontainers instead. Since 2.0.4.
 */
@Deprecated
public abstract class EmbeddedKafkaTest extends KafkaTest {

  private static EmbeddedKafkaConfig embeddedKafkaConfig(
      int kafkaPort, int zookeeperPort, int replicationFactor) {
    return EmbeddedKafkaConfig$.MODULE$.apply(
        kafkaPort,
        zookeeperPort,
        createReplicationFactorBrokerProps(replicationFactor),
        HashMap$.MODULE$.empty(),
        HashMap$.MODULE$.empty());
  }

  protected final int kafkaPort;
  protected final int replicationFactor;

  protected EmbeddedKafkaTest(ActorSystem system, int kafkaPort, int replicationFactor) {
    super(system, "localhost:" + kafkaPort);
    this.kafkaPort = kafkaPort;
    this.replicationFactor = replicationFactor;
  }

  protected EmbeddedKafkaTest(ActorSystem system, int kafkaPort) {
    this(system, kafkaPort, 1);
  }

  protected void startEmbeddedKafka(int kafkaPort, int replicationFactor) {
    EmbeddedKafka$.MODULE$.start(embeddedKafkaConfig(kafkaPort, kafkaPort + 1, replicationFactor));
  }

  protected void stopEmbeddedKafka() {
    EmbeddedKafka$.MODULE$.stop();
  }

  @BeforeAll
  void setupEmbeddedKafka() {
    startEmbeddedKafka(kafkaPort, replicationFactor);
  }

  @AfterAll
  void stopEmbeddedKafkaNow() {
    stopEmbeddedKafka();
  }
}
