/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.testkit.javadsl.EmbeddedKafkaTest;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ProducerExampleTest extends EmbeddedKafkaTest {

  private static final ActorSystem system = ActorSystem.create("ProducerExampleTest");
  private static final Materializer materializer = ActorMaterializer.create(system);
  private static final int kafkaPort = KafkaPorts.JavaProducerExamples();
  private final ExecutorService ec = Executors.newSingleThreadExecutor();
  private final ProducerSettings<String, String> producerSettings = producerDefaults();

  @Override
  public ActorSystem system() {
    return system;
  }

  @Override
  public Materializer materializer() {
    return materializer;
  }

  @Override
  public String bootstrapServers() {
    return "localhost:" + kafkaPort;
  }

  @BeforeClass
  public static void beforeClass() {
    startEmbeddedKafka(kafkaPort, 1);
  }

  @After
  public void after() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @AfterClass
  public static void afterClass() {
    stopEmbeddedKafka();
    TestKit.shutdownActorSystem(system);
  }

  void createProducer() {
    // #producer
    // #settings
    final Config config = system.settings().config().getConfig("akka.kafka.producer");
    final ProducerSettings<String, String> producerSettings =
        ProducerSettings.create(config, new StringSerializer(), new StringSerializer())
            .withBootstrapServers("localhost:9092");
    // #settings
    final org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer =
        producerSettings.createKafkaProducer();
    // #producer
  }

  @Test
  public void plainSink() throws Exception {
    String topic = createTopic(1, 1, 1);
    // #plainSink
    CompletionStage<Done> done =
        Source.range(1, 100)
            .map(number -> number.toString())
            .map(value -> new ProducerRecord<String, String>(topic, value))
            .runWith(Producer.plainSink(producerSettings), materializer);
    // #plainSink

    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control = consumeString(topic, 100);
    assertEquals(Done.done(), resultOf(done));
    assertEquals(Done.done(), resultOf(control.isShutdown()));
    CompletionStage<List<ConsumerRecord<String, String>>> result = control.drainAndShutdown(ec);
    assertEquals(100, resultOf(result).size());
  }

  @Test
  public void plainSinkWithSharedProducer() throws Exception {
    String topic = createTopic(1, 1, 1);
    final org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer =
        producerSettings.createKafkaProducer();
    // #plainSinkWithProducer
    CompletionStage<Done> done =
        Source.range(1, 100)
            .map(number -> number.toString())
            .map(value -> new ProducerRecord<String, String>(topic, value))
            .runWith(Producer.plainSink(producerSettings, kafkaProducer), materializer);
    // #plainSinkWithProducer

    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control = consumeString(topic, 100);
    assertEquals(Done.done(), resultOf(done));
    assertEquals(Done.done(), resultOf(control.isShutdown()));
    CompletionStage<List<ConsumerRecord<String, String>>> result = control.drainAndShutdown(ec);
    assertEquals(100, resultOf(result).size());

    kafkaProducer.close();
  }

  @Test
  public void observeMetrics() throws Exception {
    final org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer =
        producerSettings.createKafkaProducer();
    // #producerMetrics
    Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics =
        kafkaProducer.metrics(); // observe metrics
    // #producerMetrics
    assertFalse(metrics.isEmpty());
    kafkaProducer.close();
  }

  <KeyType, ValueType, PassThroughType>
      ProducerMessage.Envelope<KeyType, ValueType, PassThroughType> createMessage(
          KeyType key, ValueType value, PassThroughType passThrough) {
    // #singleMessage
    ProducerMessage.Envelope<KeyType, ValueType, PassThroughType> message =
        ProducerMessage.single(new ProducerRecord<>("topicName", key, value), passThrough);
    // #singleMessage
    return message;
  }

  <KeyType, ValueType, PassThroughType>
      ProducerMessage.Envelope<KeyType, ValueType, PassThroughType> createMultiMessage(
          KeyType key, ValueType value, PassThroughType passThrough) {
    // #multiMessage
    ProducerMessage.Envelope<KeyType, ValueType, PassThroughType> multiMessage =
        ProducerMessage.multi(
            Arrays.asList(
                new ProducerRecord<>("topicName", key, value),
                new ProducerRecord<>("anotherTopic", key, value)),
            passThrough);
    // #multiMessage
    return multiMessage;
  }

  <KeyType, ValueType, PassThroughType>
      ProducerMessage.Envelope<KeyType, ValueType, PassThroughType> createPassThroughMessage(
          KeyType key, ValueType value, PassThroughType passThrough) {
    // #passThroughMessage
    ProducerMessage.Envelope<KeyType, ValueType, PassThroughType> ptm =
        ProducerMessage.passThrough(passThrough);
    // #passThroughMessage
    return ptm;
  }

  @Test
  public void producerFlowExample() throws Exception {
    String topic = createTopic(1, 1, 1);
    // #flow
    CompletionStage<Done> done =
        Source.range(1, 100)
            .map(
                number -> {
                  int partition = 0;
                  String value = String.valueOf(number);
                  ProducerMessage.Envelope<String, String, Integer> msg =
                      ProducerMessage.single(
                          new ProducerRecord<>(topic, partition, "key", value), number);
                  return msg;
                })
            .via(Producer.flexiFlow(producerSettings))
            .map(
                result -> {
                  if (result instanceof ProducerMessage.Result) {
                    ProducerMessage.Result<String, String, Integer> res =
                        (ProducerMessage.Result<String, String, Integer>) result;
                    ProducerRecord<String, String> record = res.message().record();
                    RecordMetadata meta = res.metadata();
                    return meta.topic()
                        + "/"
                        + meta.partition()
                        + " "
                        + res.offset()
                        + ": "
                        + record.value();
                  } else if (result instanceof ProducerMessage.MultiResult) {
                    ProducerMessage.MultiResult<String, String, Integer> res =
                        (ProducerMessage.MultiResult<String, String, Integer>) result;
                    return res.getParts()
                        .stream()
                        .map(
                            part -> {
                              RecordMetadata meta = part.metadata();
                              return meta.topic()
                                  + "/"
                                  + meta.partition()
                                  + " "
                                  + part.metadata().offset()
                                  + ": "
                                  + part.record().value();
                            })
                        .reduce((acc, s) -> acc + ", " + s);
                  } else {
                    return "passed through";
                  }
                })
            .runWith(Sink.foreach(System.out::println), materializer);
    // #flow

    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control = consumeString(topic, 100L);
    assertEquals(Done.done(), resultOf(done));
    assertEquals(Done.done(), resultOf(control.isShutdown()));
    CompletionStage<List<ConsumerRecord<String, String>>> result = control.drainAndShutdown(ec);
    assertEquals(100, resultOf(result).size());
  }
}
