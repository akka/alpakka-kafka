/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import net.manub.embeddedkafka.EmbeddedKafkaConfig$;
import org.junit.After;
import org.junit.Before;
import scala.collection.immutable.HashMap$;

/**
 * JUnit 4 base-class with some convenience for creating an embedded Kafka broker before running the
 * tests.
 */
public abstract class EmbeddedKafkaJunit4Test extends KafkaJunit4Test {

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

  public EmbeddedKafkaJunit4Test(
      ActorSystem system, Materializer materializer, int kafkaPort, int replicationFactor) {
    super(system, materializer, "localhost:" + kafkaPort);
    this.kafkaPort = kafkaPort;
    this.replicationFactor = replicationFactor;
  }

  protected EmbeddedKafkaJunit4Test(ActorSystem system, Materializer materializer, int kafkaPort) {
    this(system, materializer, kafkaPort, 1);
  }

  protected static void startEmbeddedKafka(int kafkaPort, int replicationFactor) {
    EmbeddedKafka$.MODULE$.start(embeddedKafkaConfig(kafkaPort, kafkaPort + 1, replicationFactor));
  }

  protected static void stopEmbeddedKafka() {
    EmbeddedKafka$.MODULE$.stop();
  }

  @Before
  public void setupEmbeddedKafka() {
    EmbeddedKafkaJunit4Test.startEmbeddedKafka(kafkaPort, replicationFactor);
    setUpAdminClient();
  }

  @After
  public void cleanUpEmbeddedKafka() {
    cleanUpAdminClient();
    EmbeddedKafkaJunit4Test.stopEmbeddedKafka();
  }
}
