/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.internal.TestcontainersKafka;
import akka.stream.Materializer;

/**
 * JUnit 5 base class using [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka
 * broker in a Docker container. The Kafka broker will be kept around across multiple test classes,
 * unless `stopKafka()` is called (eg. from an `@AfterAll`-annotated method.
 *
 * <p>Extending classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)` to create a
 * single instance of the test class with `@BeforeAll` and `@AfterAll` annotated methods called by
 * the test framework.
 *
 * <p>The Testcontainers dependency has to be added explicitly.
 */
public abstract class TestcontainersKafkaTest extends KafkaTest {

  public static final KafkaTestkitTestcontainersSettings settings =
      TestcontainersKafka.Singleton().testcontainersSettings();

  protected TestcontainersKafkaTest(ActorSystem system, Materializer materializer) {
    super(system, materializer, startKafka(settings));
  }

  protected TestcontainersKafkaTest(
      ActorSystem system, Materializer materializer, KafkaTestkitTestcontainersSettings settings) {
    super(system, materializer, startKafka(settings));
  }

  @Deprecated
  protected TestcontainersKafkaTest(
      ActorSystem system, Materializer materializer, String confluentPlatformVersion) {
    super(system, materializer, startKafka(confluentPlatformVersion));
  }

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
}
