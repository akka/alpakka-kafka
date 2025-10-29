/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.kafka.tests.javadsl.LogCapturingExtension;
import akka.stream.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
// #jackson-imports
import com.fasterxml.jackson.core.JsonParseException;
// #jackson-imports
// #protobuf-imports
// the Protobuf generated class
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import docs.javadsl.proto.OrderMessages;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
// #protobuf-imports
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(LogCapturingExtension.class)
public class SerializationTest extends TestcontainersKafkaTest {

  private static final ActorSystem sys = ActorSystem.create("SerializationTest");
  private static final Executor ec = Executors.newSingleThreadExecutor();

  public SerializationTest() {
    super(sys);
  }

  @AfterAll
  void afterAll() {
    TestKit.shutdownActorSystem(sys);
  }

  @Test
  public void jacksonDeSer() throws Exception {
    final String topic = createTopic();
    final String group = createGroupId();

    ConsumerSettings<String, String> consumerSettings = consumerDefaults().withGroupId(group);

    SampleData sample = new SampleData("Viktor", 45);
    List<SampleData> samples = Arrays.asList(sample, sample, sample);

    // #jackson-serializer #jackson-deserializer

    final ObjectMapper mapper = new ObjectMapper();
    // #jackson-serializer #jackson-deserializer
    // #jackson-serializer
    final ObjectWriter sampleDataWriter = mapper.writerFor(SampleData.class);

    CompletionStage<Done> producerCompletion =
        Source.from(samples)
            .map(sampleDataWriter::writeValueAsString)
            .map(json -> new ProducerRecord<String, String>(topic, json))
            .runWith(Producer.plainSink(producerDefaults()), sys);
    // #jackson-serializer

    CompletionStage<Done> badlyFormattedProducer =
        Source.single("{ this is no sample data")
            .map(json -> new ProducerRecord<String, String>(topic, json))
            .runWith(Producer.plainSink(producerDefaults()), sys);

    // #jackson-deserializer
    final ObjectReader sampleDataReader = mapper.readerFor(SampleData.class);

    final Attributes resumeOnParseException =
        ActorAttributes.withSupervisionStrategy(
            exception -> {
              if (exception instanceof JsonParseException) {
                return Supervision.resume();
              } else {
                return Supervision.stop();
              }
            });

    Consumer.DrainingControl<List<SampleData>> control =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
            .map(ConsumerRecord::value)
            .<SampleData>map(sampleDataReader::readValue)
            .withAttributes(resumeOnParseException) // drop faulty elements
            // #jackson-deserializer
            .take(samples.size())
            // #jackson-deserializer
            .toMat(Sink.seq(), Consumer::createDrainingControl)
            .run(sys);
    // #jackson-deserializer

    assertThat(
        producerCompletion.toCompletableFuture().get(4, TimeUnit.SECONDS), is(Done.getInstance()));
    assertThat(
        badlyFormattedProducer.toCompletableFuture().get(4, TimeUnit.SECONDS),
        is(Done.getInstance()));
    assertThat(
        control.isShutdown().toCompletableFuture().get(10, TimeUnit.SECONDS),
        is(Done.getInstance()));

    List<SampleData> result =
        control.drainAndShutdown(ec).toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertThat(result, is(samples));
    assertThat(result.size(), is(samples.size()));
  }

  @Test
  public void protobufDeSer() throws Exception {
    final String topic = createTopic();
    final String group = createGroupId();

    OrderMessages.Order sample = OrderMessages.Order.newBuilder().setId("789465").build();
    List<OrderMessages.Order> samples = Arrays.asList(sample, sample, sample);

    // #protobuf-serializer

    ProducerSettings<String, byte[]> producerSettings = // ...
        // #protobuf-serializer
        producerDefaults(new StringSerializer(), new ByteArraySerializer());

    // #protobuf-serializer

    CompletionStage<Done> producerCompletion =
        Source.from(samples)
            .map(order -> new ProducerRecord<>(topic, order.getId(), order.toByteArray()))
            .runWith(Producer.plainSink(producerSettings), sys);
    // #protobuf-serializer

    CompletionStage<Done> badlyFormattedProducer =
        Source.single("faulty".getBytes(StandardCharsets.UTF_8))
            .map(data -> new ProducerRecord<>(topic, "32", data))
            .runWith(Producer.plainSink(producerSettings), sys);

    // #protobuf-deserializer

    final Attributes resumeOnParseException =
        ActorAttributes.withSupervisionStrategy(
            exception -> {
              if (exception instanceof com.google.protobuf.InvalidProtocolBufferException) {
                return Supervision.resume();
              } else {
                return Supervision.stop();
              }
            });

    ConsumerSettings<String, byte[]> consumerSettings = // ...
        // #protobuf-deserializer
        consumerDefaults(new StringDeserializer(), new ByteArrayDeserializer()).withGroupId(group);
    // #protobuf-deserializer

    Consumer.DrainingControl<List<OrderMessages.Order>> control =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
            .map(ConsumerRecord::value)
            .map(OrderMessages.Order::parseFrom)
            .withAttributes(resumeOnParseException) // drop faulty elements
            // #protobuf-deserializer
            .take(samples.size())
            // #protobuf-deserializer
            .toMat(Sink.seq(), Consumer::createDrainingControl)
            .run(sys);
    // #protobuf-deserializer

    assertThat(
        producerCompletion.toCompletableFuture().get(4, TimeUnit.SECONDS), is(Done.getInstance()));
    assertThat(
        badlyFormattedProducer.toCompletableFuture().get(4, TimeUnit.SECONDS),
        is(Done.getInstance()));
    assertThat(
        control.isShutdown().toCompletableFuture().get(10, TimeUnit.SECONDS),
        is(Done.getInstance()));

    List<OrderMessages.Order> result =
        control.drainAndShutdown(ec).toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertThat(result, is(samples));
    assertThat(result.size(), is(samples.size()));
  }
}
