/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.testkit.internal.TestcontainersKafkaHelper;
import akka.stream.Materializer;
import org.junit.After;
import org.junit.Before;

/**
 * JUnit 4 base class using [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka
 * broker in a Docker container. The Kafka broker will be kept around across multiple test classes,
 * unless `stopKafkaBroker()` is called.
 *
 * <p>The Testcontainers dependency has to be added explicitly.
 */
public abstract class TestcontainersKafkaJunit4Test extends KafkaJunit4Test {

  public static final String confluentPlatformVersionDefault =
      TestcontainersKafkaHelper.ConfluentPlatformVersionDefault();

  protected TestcontainersKafkaJunit4Test(ActorSystem system, Materializer materializer) {
    super(system, materializer, startKafka(confluentPlatformVersionDefault));
  }

  protected TestcontainersKafkaJunit4Test(
      ActorSystem system, Materializer materializer, String confluentPlatformVersion) {
    super(system, materializer, startKafka(confluentPlatformVersion));
  }

  protected static String startKafka(String confluentPlatformVersion) {
    return TestcontainersKafkaHelper.startKafka(confluentPlatformVersion);
  }

  protected static void stopKafka() {
    TestcontainersKafkaHelper.stopKafka();
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
}
