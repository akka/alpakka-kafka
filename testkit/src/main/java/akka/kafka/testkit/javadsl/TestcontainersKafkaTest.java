/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.actor.ClassicActorSystemProvider;
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

  /**
   * @deprecated Materializer no longer necessary in Akka 2.6, use
   *     `TestcontainersKafkaTest(ClassicActorSystemProvider)` instead, since 2.1.0
   */
  @Deprecated
  protected TestcontainersKafkaTest(ActorSystem system, Materializer mat) {
    super(system, mat, startKafka(settings));
  }

  protected TestcontainersKafkaTest(ClassicActorSystemProvider system) {
    super(system, startKafka(settings));
  }

  protected TestcontainersKafkaTest(
      ActorSystem system, KafkaTestkitTestcontainersSettings settings) {
    super(system, startKafka(settings));
  }

  protected static String startKafka(KafkaTestkitTestcontainersSettings settings) {
    return TestcontainersKafka.Singleton().startCluster(settings);
  }

  protected static void stopKafka() {
    TestcontainersKafka.Singleton().stopCluster();
  }

  protected String getSchemaRegistryUrl() {
    return TestcontainersKafka.Singleton().getSchemaRegistryUrl();
  }
}
