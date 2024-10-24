/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.SendProducer;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.kafka.tests.javadsl.LogCapturingExtension;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(LogCapturingExtension.class)
public class SendProducerTest extends TestcontainersKafkaTest {

  private static final ActorSystem system = ActorSystem.create("ElementProducerTest");

  public SendProducerTest() {
    super(system);
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
    SendProducer<String, String> producer = new SendProducer<>(producerSettings, system);
    try {
      CompletionStage<RecordMetadata> result =
          producer.send(new ProducerRecord<>(topic, "key", "value"));
      // Blocking here for illustration only, you need to handle the future result
      RecordMetadata recordMetadata = result.toCompletableFuture().get(2, TimeUnit.SECONDS);
      // #record
      assertThat(recordMetadata.topic(), is(topic));
      // #record
    } finally {
      producer.close().toCompletableFuture().get(1, TimeUnit.MINUTES);
    }
    // #record
    ConsumerRecord<String, String> consumed =
        resultOf(consumeString(topic, 1).streamCompletion().thenApply(list -> list.get(0)));
    assertThat(consumed.value(), is("value"));
  }

  @Test
  public void multiMessage() throws Exception {
    String topic = createTopic();

    ProducerSettings<String, String> producerSettings = producerDefaults();
    // #multiMessage
    SendProducer<String, String> producer = new SendProducer<>(producerSettings, system);
    try {
      ProducerMessage.Envelope<String, String, String> envelope =
          ProducerMessage.multi(
              Arrays.asList(
                  new ProducerRecord<>(topic, "key", "value1"),
                  new ProducerRecord<>(topic, "key", "value2"),
                  new ProducerRecord<>(topic, "key", "value3")),
              "context");
      CompletionStage<ProducerMessage.Results<String, String, String>> send =
          producer.sendEnvelope(envelope);
      // Blocking here for illustration only, you need to handle the future result
      ProducerMessage.Results<String, String, String> result =
          send.toCompletableFuture().get(2, TimeUnit.SECONDS);
      // #multiMessage
      assertThat(result, isA(ProducerMessage.MultiResult.class));
      ProducerMessage.MultiResult<String, String, String> result1 =
          (ProducerMessage.MultiResult<String, String, String>) result;
      assertThat(result1.getParts(), hasSize(3));
      // #multiMessage
    } finally {
      producer.close().toCompletableFuture().get(1, TimeUnit.MINUTES);
    }
    // #multiMessage
    List<ConsumerRecord<String, String>> consumed =
        resultOf(consumeString(topic, 3).streamCompletion());
    assertThat(consumed, hasSize(3));
    assertThat(consumed.get(0).value(), is("value1"));
  }
}
