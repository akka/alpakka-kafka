package docs.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

// #testcontainers-settings
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestkitTestcontainersTest extends TestcontainersKafkaTest {

    private static final ActorSystem system = ActorSystem.create("TestkitTestcontainersTest");
    private static final Materializer materializer = ActorMaterializer.create(system);

    private static KafkaTestkitTestcontainersSettings testcontainersSettings =
            KafkaTestkitTestcontainersSettings.create(system)
                .withNumBrokers(3)
                .withInternalTopicsReplicationFactor(2);

    TestkitTestcontainersTest() {
        super(system, materializer, testcontainersSettings);
    }

    // ...

    @AfterAll
    void afterClass() {
        TestcontainersKafkaTest.stopKafka();
    }
}
// #testcontainers-settings