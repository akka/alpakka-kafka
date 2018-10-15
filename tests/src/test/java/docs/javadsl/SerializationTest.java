/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaPorts;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.EmbeddedKafkaWithSchemaRegistryTest;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SerializationTest extends EmbeddedKafkaWithSchemaRegistryTest {

  private static final ActorSystem sys = ActorSystem.create("AssignmentTest");
  private static final Materializer mat = ActorMaterializer.create(sys);

  @Override
  public ActorSystem system() {
    return sys;
  }

  static final int kafkaPort = KafkaPorts.SerializationTest();
  static final int zooKeeperPort = KafkaPorts.SerializationTest() + 1;
  static final int schemaRegistryPort = KafkaPorts.SerializationTest() + 2;

  @Override
  public String bootstrapServers() {
    return "localhost:" + kafkaPort;
  }

  final String schemaRegistryUrl = "http://localhost:" + schemaRegistryPort;

  @BeforeClass
  public static void beforeClass() {
    // Schema registry uses Glassfish which uses java.util.logging
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    startEmbeddedKafka(kafkaPort, zooKeeperPort, schemaRegistryPort, 1);
  }

  @Test
  public void avroDeSerMustWorkWithSchemaRegistry() throws Exception {
    final String topic = createTopic(0, 1, 1);
    final String group = createGroupId(0);

    // #serializer #de-serializer

    Map<String, Object> kafkaAvroSerDeConfig = new HashMap<>();
    kafkaAvroSerDeConfig.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
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

    Pair<Consumer.Control, CompletionStage<List<ConsumerRecord<String, Object>>>>
        controlCompletionStagePair =
            Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
                .take(samples.size())
                .toMat(Sink.seq(), Keep.both())
                .run(mat);
    // #de-serializer

    assertThat(
        controlCompletionStagePair
            .first()
            .isShutdown()
            .toCompletableFuture()
            .get(4, TimeUnit.SECONDS),
        is(Done.getInstance()));
    assertThat(
        controlCompletionStagePair.second().toCompletableFuture().get(1, TimeUnit.SECONDS).size(),
        is(samples.size()));
  }

  @After
  public void after() {
    StreamTestKit.assertAllStagesStopped(mat);
  }

  @AfterClass
  public static void afterClass() {
    stopEmbeddedKafka();
    TestKit.shutdownActorSystem(sys);
  }
}
