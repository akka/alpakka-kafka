/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.stream.testkit.javadsl.StreamTestKit;
import org.junit.After;
import org.junit.Before;

/**
 * JUnit 5 aka Jupiter base-class with some convenience for accessing a Kafka broker. Extending
 * classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)` to create a single instance
 * of the test class with `@BeforeAll` and `@AfterAll` annotated methods called by the test
 * framework.
 */
public abstract class KafkaJunit4Test extends BaseKafkaTest {

  @Before
  public void setUpAdmin() {
    setUpAdminClient();
  }

  @After
  public void cleanUpAdmin() {
    cleanUpAdminClient();
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer());
  }
}
