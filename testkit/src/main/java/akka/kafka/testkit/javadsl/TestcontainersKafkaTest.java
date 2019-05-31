/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.testkit.internal.TestcontainersKafkaHelper;
import akka.stream.Materializer;

/**
 * JUnit 5 base class using [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka
 * broker in a Docker container. The Kafka broker will be kept around across multiple test classes,
 * unless `stopKafkaBroker()` is called (eg. from an `@AfterAll`-annotated method.
 *
 * <p>Extending classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)` to create a
 * single instance of the test class with `@BeforeAll` and `@AfterAll` annotated methods called by
 * the test framework.
 *
 * <p>The Testcontainers dependency has to be added explicitly.
 */
public abstract class TestcontainersKafkaTest extends KafkaTest {

  public static final String confluentPlatformVersionDefault =
      TestcontainersKafkaHelper.ConfluentPlatformVersionDefault();

  protected TestcontainersKafkaTest(ActorSystem system, Materializer materializer) {
    super(system, materializer, startKafka(confluentPlatformVersionDefault));
  }

  protected TestcontainersKafkaTest(
      ActorSystem system, Materializer materializer, String confluentPlatformVersion) {
    super(system, materializer, startKafka(confluentPlatformVersion));
  }

  protected static String startKafka(String confluentPlatformVersion) {
    return TestcontainersKafkaHelper.startKafka(confluentPlatformVersion);
  }

  protected static void stopKafka() {
    TestcontainersKafkaHelper.stopKafka();
  }
}
