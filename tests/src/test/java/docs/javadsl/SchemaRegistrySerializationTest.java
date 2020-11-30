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
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import docs.scaladsl.SampleAvroClass;
// #imports
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
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
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

// #schema-registry-settings
public class SchemaRegistrySerializationTest extends TestcontainersKafkaJunit4Test {

  private static final ActorSystem sys = ActorSystem.create("SchemaRegistrySerializationTest");
  private static final Executor ec = Executors.newSingleThreadExecutor();

  public SchemaRegistrySerializationTest() {
    // #schema-registry-settings
    // NOTE: Overriding KafkaTestkitTestcontainersSettings doesn't necessarily do anything here
    // because the JUnit testcontainer abstract classes run the testcontainers as a singleton.
    // Whatever JUnit test spawns first is the only one that can override settings. To workaround
    // this I've enabled the schema registry container for all tests in an application.conf.
    // #schema-registry-settings
    super(
        sys,
        KafkaTestkitTestcontainersSettings.create(sys)
            .withInternalTopicsReplicationFactor(1)
            .withSchemaRegistry(true));
  }

  // #schema-registry-settings

  @Test
  public void avroDeSerMustWorkWithSchemaRegistry() throws Exception {
    final String topic = createTopic();
    final String group = createGroupId();

    // #serializer #de-serializer

    Map<String, Object> kafkaAvroSerDeConfig = new HashMap<>();
    kafkaAvroSerDeConfig.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
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
            .runWith(Producer.plainSink(producerSettings), sys);
    // #serializer

    // #de-serializer

    Consumer.DrainingControl<List<ConsumerRecord<String, Object>>> controlCompletionStagePair =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
            .take(samples.size())
            .toMat(Sink.seq(), Consumer::createDrainingControl)
            .run(sys);
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
