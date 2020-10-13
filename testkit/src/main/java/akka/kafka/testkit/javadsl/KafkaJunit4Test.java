/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.testkit.javadsl.StreamTestKit;
import org.junit.After;
import org.junit.Before;

/** JUnit 4 base-class with some convenience for accessing a Kafka broker. */
public abstract class KafkaJunit4Test extends BaseKafkaTest {

  protected KafkaJunit4Test(ActorSystem system, String bootstrapServers) {
    super(system, bootstrapServers);
  }

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
    // you might need to configure `stop-timeout` in your `application.conf`
    // as the default of 30s will fail this
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(system()));
  }
}
