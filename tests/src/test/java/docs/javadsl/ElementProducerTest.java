/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.ElementProducer;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.kafka.tests.javadsl.LogCapturingExtension;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(LogCapturingExtension.class)
public class ElementProducerTest extends TestcontainersKafkaTest {

  private static final ActorSystem system = ActorSystem.create("ElementProducerTest");
  private static final Materializer materializer = ActorMaterializer.create(system);
  private static final Executor executor = Executors.newSingleThreadExecutor();

  public ElementProducerTest() {
    super(system, materializer);
  }

  @AfterAll
  void afterClass() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void consumeOneProduceMany() throws Exception {
    String topic = createTopic();

    ProducerSettings<String, String> producerSettings = producerDefaults();
    try (ElementProducer<String, String> producer =
        new ElementProducer<>(producerSettings, executor)) {
      CompletionStage<RecordMetadata> result =
          producer.send(new ProducerRecord<>(topic, "key", "value"));
      RecordMetadata recordMetadata = resultOf(result);
      assertThat(recordMetadata.topic(), is(topic));
    }
    ConsumerRecord<String, String> consumed =
        resultOf(consumeString(topic, 1).streamCompletion().thenApply(list -> list.get(0)));
    assertThat(consumed.value(), is("value"));
  }
}
