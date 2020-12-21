/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.internal.TestcontainersKafka;
import akka.stream.Materializer;
import org.junit.After;
import org.junit.Before;

/**
 * JUnit 4 base class using [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka
 * broker in a Docker container. The Kafka broker will be kept around across multiple test classes,
 * unless `stopKafka()` is called.
 *
 * <p>The Testcontainers dependency has to be added explicitly.
 */
public abstract class TestcontainersKafkaJunit4Test extends KafkaJunit4Test {

  private static final KafkaTestkitTestcontainersSettings settings =
      TestcontainersKafka.Singleton().testcontainersSettings();

  protected TestcontainersKafkaJunit4Test(ActorSystem system, Materializer materializer) {
    super(system, materializer, startKafka(settings));
  }

  protected TestcontainersKafkaJunit4Test(
      ActorSystem system, Materializer materializer, KafkaTestkitTestcontainersSettings settings) {
    super(system, materializer, startKafka(settings));
  }

  protected static String startKafka(KafkaTestkitTestcontainersSettings settings) {
    return TestcontainersKafka.Singleton().startCluster(settings);
  }

  protected static void stopKafka() {
    TestcontainersKafka.Singleton().stopCluster();
  }

  @Before
  @Override
  public void setUpAdminClient() {
    super.setUpAdminClient();
  }

  @After
  @Override
  public void cleanUpAdminClient() {
    super.cleanUpAdminClient();
  }

  protected String getSchemaRegistryUrl() {
    return TestcontainersKafka.Singleton().getSchemaRegistryUrl();
  }
}
