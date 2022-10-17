/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.actor.ActorSystem;
import akka.actor.ClassicActorSystemProvider;
import akka.stream.Materializer;
import akka.stream.testkit.javadsl.StreamTestKit;
import org.junit.After;
import org.junit.Before;

/** JUnit 4 base-class with some convenience for accessing a Kafka broker. */
public abstract class KafkaJunit4Test extends BaseKafkaTest {

  /**
   * @deprecated Materializer no longer necessary in Akka 2.6, use
   *     `KafkaJunit4Test(ClassicActorSystemProvider, String)` instead, since 2.1.0
   */
  @Deprecated
  protected KafkaJunit4Test(ActorSystem system, Materializer mat, String bootstrapServers) {
    super(system, mat, bootstrapServers);
  }

  protected KafkaJunit4Test(ClassicActorSystemProvider system, String bootstrapServers) {
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
    StreamTestKit.assertAllStagesStopped(materializer);
  }
}
