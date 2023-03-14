/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #testcontainers-settings
import akka.actor.ActorSystem;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestkitTestcontainersTest extends TestcontainersKafkaTest {

  private static final ActorSystem system = ActorSystem.create("TestkitTestcontainersTest");

  private static KafkaTestkitTestcontainersSettings testcontainersSettings =
      KafkaTestkitTestcontainersSettings.create(system)
          .withNumBrokers(3)
          .withInternalTopicsReplicationFactor(2)
          .withConfigureKafkaConsumer(
              brokerContainers ->
                  brokerContainers.forEach(
                      b -> b.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")));

  TestkitTestcontainersTest() {
    // this will only start a new cluster if it has not already been started.
    //
    // you must stop the cluster in the afterClass implementation if you want to create a cluster
    // per test class
    // using (TestInstance.Lifecycle.PER_CLASS)
    super(system, testcontainersSettings);
  }

  // ...

  // omit this implementation if you want the cluster to stay up for all your tests
  @AfterAll
  void afterClass() {
    TestcontainersKafkaTest.stopKafka();
  }
}
// #testcontainers-settings
