/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.testkit.javadsl.StreamTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

/**
 * JUnit 5 aka Jupiter base-class with some convenience for accessing a Kafka broker. Extending
 * classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)` to create a single instance
 * of the test class with `@BeforeAll` and `@AfterAll` annotated methods called by the test
 * framework.
 */
public abstract class KafkaTest extends BaseKafkaTest {

  protected KafkaTest(ActorSystem system, String bootstrapServers) {
    super(system, bootstrapServers);
  }

  @BeforeAll
  public void setupAdmin() {
    setUpAdminClient();
  }

  @AfterAll
  public void cleanUpAdmin() {
    cleanUpAdminClient();
  }

  @AfterEach
  public void checkForStageLeaks() {
    // you might need to configure `stop-timeout` in your `application.conf`
    // as the default of 30s will fail this
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(system()));
  }
}
