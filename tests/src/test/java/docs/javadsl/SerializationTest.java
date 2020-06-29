/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.stream.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
// #jackson-imports
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.core.JsonParseException;
// #jackson-imports
import docs.scaladsl.SampleAvroClass;
// #imports
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
// #imports
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
// #imports
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
// #imports
import org.junit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

// #schema-registry-settings
public class SerializationTest extends TestcontainersKafkaJunit4Test {

  private static final ActorSystem sys = ActorSystem.create("SerializationTest");
  private static final Materializer mat = ActorMaterializer.create(sys);
  private static final Executor ec = Executors.newSingleThreadExecutor();

  public SerializationTest() {
    super(
        sys,
        mat,
        KafkaTestkitTestcontainersSettings.create(sys)
            .withInternalTopicsReplicationFactor(1)
            .withSchemaRegistry());
  }

  // tests..

  // #schema-registry-settings

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
            .runWith(Producer.plainSink(producerDefaults()), mat);
    // #jackson-serializer

    CompletionStage<Done> badlyFormattedProducer =
        Source.single("{ this is no sample data")
            .map(json -> new ProducerRecord<String, String>(topic, json))
            .runWith(Producer.plainSink(producerDefaults()), mat);

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
            .run(mat);
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
  public void avroDeSerMustWorkWithSchemaRegistry() throws Exception {
    final String topic = createTopic();
    final String group = createGroupId();

    // #serializer #de-serializer

    Map<String, Object> kafkaAvroSerDeConfig = new HashMap<>();
    kafkaAvroSerDeConfig.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
    // #serializer #de-serializer
    // #de-serializer
    KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig, false);
    Deserializer<Object> deserializer = kafkaAvroDeserializer;

    ConsumerSettings<String, Object> consumerSettings =
        ConsumerSettings.create(sys, new StringDeserializer(), deserializer)
            .withBootstrapServers(bootstrapServers())
            .withGroupId(group)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // #de-serializer

    // #serializer
    KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
    kafkaAvroSerializer.configure(kafkaAvroSerDeConfig, false);
    Serializer<Object> serializer = kafkaAvroSerializer;

    ProducerSettings<String, Object> producerSettings =
        ProducerSettings.create(sys, new StringSerializer(), serializer)
            .withBootstrapServers(bootstrapServers());

    SampleAvroClass sample = new SampleAvroClass("key", "name");
    List<SampleAvroClass> samples = Arrays.asList(sample, sample, sample);
    CompletionStage<Done> producerCompletion =
        Source.from(samples)
            .map(n -> new ProducerRecord<String, Object>(topic, n.key(), n))
            .runWith(Producer.plainSink(producerSettings), mat);
    // #serializer

    // #de-serializer

    Consumer.DrainingControl<List<ConsumerRecord<String, Object>>> controlCompletionStagePair =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
            .take(samples.size())
            .toMat(Sink.seq(), Consumer::createDrainingControl)
            .run(mat);
    // #de-serializer

    assertThat(
        controlCompletionStagePair.isShutdown().toCompletableFuture().get(20, TimeUnit.SECONDS),
        is(Done.getInstance()));
    List<ConsumerRecord<String, Object>> result =
        controlCompletionStagePair
            .drainAndShutdown(ec)
            .toCompletableFuture()
            .get(1, TimeUnit.SECONDS);
    assertThat(result.size(), is(samples.size()));
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(sys);
  }
  // #schema-registry-settings
}
// #schema-registry-settings
