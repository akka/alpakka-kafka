/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

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
    StreamTestKit.assertAllStagesStopped(materializer());
  }
}
