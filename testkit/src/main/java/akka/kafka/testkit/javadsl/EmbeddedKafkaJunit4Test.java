/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import net.manub.embeddedkafka.EmbeddedKafkaConfig$;
import org.junit.After;
import org.junit.Before;
import scala.collection.immutable.HashMap$;

/**
 * JUnit 5 aka Jupiter base-class with some convenience for creating an embedded Kafka broker before
 * running the tests. Extending classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)`
 * to create a single instance of the test class with `@BeforeAll` and `@AfterAll` annotated methods
 * called by the test framework.
 */
public abstract class EmbeddedKafkaJunit4Test extends KafkaTest {

  private static EmbeddedKafkaConfig embeddedKafkaConfig(
      int kafkaPort, int zookeeperPort, int replicationFactor) {
    return EmbeddedKafkaConfig$.MODULE$.apply(
        kafkaPort,
        zookeeperPort,
        createReplicationFactorBrokerProps(replicationFactor),
        HashMap$.MODULE$.empty(),
        HashMap$.MODULE$.empty());
  }

  protected static void startEmbeddedKafka(int kafkaPort, int replicationFactor) {
    EmbeddedKafka$.MODULE$.start(embeddedKafkaConfig(kafkaPort, kafkaPort + 1, replicationFactor));
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
    EmbeddedKafkaJunit4Test.startEmbeddedKafka(kafkaPort(), replicationFactor());
    setUpAdminClient();
  }

  @After
  public void cleanUpEmbeddedKafka() {
    cleanUpAdminClient();
    EmbeddedKafkaJunit4Test.stopEmbeddedKafka();
  }
}
