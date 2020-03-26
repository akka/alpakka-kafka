/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

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
  public void record() throws Exception {
    String topic = createTopic();

    ProducerSettings<String, String> producerSettings = producerDefaults();
    // #record
    try (ElementProducer<String, String> producer =
        new ElementProducer<>(producerSettings, executor)) {
      CompletionStage<RecordMetadata> result =
          producer.send(new ProducerRecord<>(topic, "key", "value"));
      // #record
      RecordMetadata recordMetadata = resultOf(result);
      assertThat(recordMetadata.topic(), is(topic));
      // #record
    }
    // #record
    ConsumerRecord<String, String> consumed =
        resultOf(consumeString(topic, 1).streamCompletion().thenApply(list -> list.get(0)));
    assertThat(consumed.value(), is("value"));
  }

  @Test
  public void message() throws Exception {
    String topic = createTopic();

    ProducerSettings<String, String> producerSettings = producerDefaults();
    // #message
    try (ElementProducer<String, String> producer =
        new ElementProducer<>(producerSettings, executor)) {
      CompletionStage<ProducerMessage.Result<String, String, String>> send =
          producer.sendMessage(
              new ProducerMessage.Message<>(
                  new ProducerRecord<>(topic, "key", "value"), "context"));
      // #message
      ProducerMessage.Result<String, String, String> result = resultOf(send);
      assertThat(result.metadata().topic(), is(topic));
      // #message
    }
    // #message
    ConsumerRecord<String, String> consumed =
        resultOf(consumeString(topic, 1).streamCompletion().thenApply(list -> list.get(0)));
    assertThat(consumed.value(), is("value"));
  }

  @Test
  public void multiMessage() throws Exception {
    String topic = createTopic();

    ProducerSettings<String, String> producerSettings = producerDefaults();
    // #multiMessage
    try (ElementProducer<String, String> producer =
        new ElementProducer<>(producerSettings, executor)) {
      ProducerMessage.Envelope<String, String, String> envelope =
          ProducerMessage.multi(
              Arrays.asList(
                  new ProducerRecord<>(topic, "key", "value1"),
                  new ProducerRecord<>(topic, "key", "value2"),
                  new ProducerRecord<>(topic, "key", "value3")),
              "context");
      CompletionStage<ProducerMessage.Results<String, String, String>> send =
          producer.sendEnvelope(envelope);
      // #multiMessage
      ProducerMessage.Results<String, String, String> result = resultOf(send);
      assertThat(result, isA(ProducerMessage.MultiResult.class));
      ProducerMessage.MultiResult<String, String, String> result1 =
          (ProducerMessage.MultiResult<String, String, String>) result;
      assertThat(result1.getParts(), hasSize(3));
      // #multiMessage
    }
    // #multiMessage
    List<ConsumerRecord<String, String>> consumed =
        resultOf(consumeString(topic, 3).streamCompletion());
    assertThat(consumed, hasSize(3));
    assertThat(consumed.get(0).value(), is("value1"));
  }
}
