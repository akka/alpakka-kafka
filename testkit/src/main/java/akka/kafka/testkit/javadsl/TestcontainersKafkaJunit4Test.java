/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.internal.TestcontainersKafka;
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

  protected TestcontainersKafkaJunit4Test(ActorSystem system) {
    super(system, startKafka(settings));
  }

  /** @deprecated Use constructor with `testcontainersSettings` instead. since 2.0.0 */
  @Deprecated
  protected TestcontainersKafkaJunit4Test(ActorSystem system, String confluentPlatformVersion) {
    super(system, startKafka(confluentPlatformVersion));
  }

  protected TestcontainersKafkaJunit4Test(
      ActorSystem system, KafkaTestkitTestcontainersSettings settings) {
    super(system, startKafka(settings));
  }

  /** @deprecated Use method with `testcontainersSettings` instead. since 2.0.0 */
  @Deprecated
  protected static String startKafka(String confluentPlatformVersion) {
    KafkaTestkitTestcontainersSettings settings =
        TestcontainersKafka.Singleton()
            .testcontainersSettings()
            .withConfluentPlatformVersion(confluentPlatformVersion);
    return TestcontainersKafka.Singleton().startKafka(settings);
  }

  protected static String startKafka(KafkaTestkitTestcontainersSettings settings) {
    return TestcontainersKafka.Singleton().startKafka(settings);
  }

  protected static void stopKafka() {
    TestcontainersKafka.Singleton().stopKafka();
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
