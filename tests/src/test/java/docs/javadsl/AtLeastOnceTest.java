/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.kafka.tests.javadsl.LogCapturingJunit4;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import org.junit.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

// #oneToMany #oneToConditional
import akka.Done;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.ConsumerMessage.CommittableOffset;
import akka.kafka.ProducerMessage.Envelope;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

// #oneToMany #oneToConditional

public class AtLeastOnceTest extends TestcontainersKafkaJunit4Test {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final ActorSystem system = ActorSystem.create("AtLeastOnceTest");
  private static final Materializer materializer = ActorMaterializer.create(system);
  private static final Executor ec = Executors.newSingleThreadExecutor();

  public AtLeastOnceTest() {
    super(system, materializer);
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void consumeOneProduceMany() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic1 = createTopic(1);
    String topic2 = createTopic(2);
    String topic3 = createTopic(3);
    ProducerSettings<String, String> producerSettings = producerDefaults();
    CommitterSettings committerSettings = committerDefaults();
    Consumer.DrainingControl<Done> control =
        // #oneToMany
        Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1))
            .map(
                msg -> {
                  Envelope<String, String, CommittableOffset> multiMsg =
                      ProducerMessage.multi(
                          Arrays.asList(
                              new ProducerRecord<>(topic2, msg.record().value()),
                              new ProducerRecord<>(topic3, msg.record().value())),
                          msg.committableOffset());
                  return multiMsg;
                })
            .via(Producer.flexiFlow(producerSettings))
            .map(m -> m.passThrough())
            .toMat(Committer.sink(committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #oneToMany
    Pair<Consumer.Control, CompletionStage<List<ConsumerRecord<String, String>>>> tuple =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic2, topic3))
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    produceString(topic1, 10, partition0).toCompletableFuture().get(1, TimeUnit.SECONDS);
    sleepSeconds(10, "to make produce happen");
    assertThat(
        control.drainAndShutdown(ec).toCompletableFuture().get(5, TimeUnit.SECONDS),
        is(Done.done()));
    assertThat(
        tuple.first().shutdown().toCompletableFuture().get(5, TimeUnit.SECONDS), is(Done.done()));
    assertThat(tuple.second().toCompletableFuture().get(5, TimeUnit.SECONDS).size(), is(20));
  }

  @Test
  public void consumeOneProduceManyWithOffsetContext() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(0));
    String topic1 = createTopic(1, 1, 1);
    String topic2 = createTopic(2, 1, 1);
    String topic3 = createTopic(3, 1, 1);
    ProducerSettings<String, String> producerSettings = producerDefaults();
    CommitterSettings committerSettings = committerDefaults();
    Consumer.DrainingControl<Done> control =
        Consumer.sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic1))
            .map(
                record -> {
                  Envelope<String, String, NotUsed> multiMsg =
                      ProducerMessage.multi(
                          Arrays.asList(
                              new ProducerRecord<>(topic2, record.value()),
                              new ProducerRecord<>(topic3, record.value())));
                  return multiMsg;
                })
            .via(Producer.flowWithContext(producerSettings))
            .toMat(Committer.sinkWithOffsetContext(committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    Pair<Consumer.Control, CompletionStage<List<ConsumerRecord<String, String>>>> tuple =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic2, topic3))
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    produceString(topic1, 10, partition0).toCompletableFuture().get(1, TimeUnit.SECONDS);
    sleepSeconds(10, "to make produce happen");
    assertThat(
        control.drainAndShutdown(ec).toCompletableFuture().get(5, TimeUnit.SECONDS),
        is(Done.done()));
    assertThat(
        tuple.first().shutdown().toCompletableFuture().get(5, TimeUnit.SECONDS), is(Done.done()));
    assertThat(tuple.second().toCompletableFuture().get(5, TimeUnit.SECONDS).size(), is(20));
  }

  boolean duplicate(String s) {
    return "1".equals(s);
  }

  boolean ignore(String s) {
    return "2".equals(s);
  }

  @Test
  public void consumerOneProduceConditional() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic1 = createTopic(1);
    String topic2 = createTopic(2);
    String topic3 = createTopic(3);
    String topic4 = createTopic(4);
    ProducerSettings<String, String> producerSettings = producerDefaults();
    CommitterSettings committerSettings = committerDefaults();
    Consumer.DrainingControl<Done> control =
        // #oneToConditional
        Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1))
            .map(
                msg -> {
                  final Envelope<String, String, CommittableOffset> produce;
                  if (duplicate(msg.record().value())) {
                    produce =
                        ProducerMessage.multi(
                            Arrays.asList(
                                new ProducerRecord<>(topic2, msg.record().value()),
                                new ProducerRecord<>(topic3, msg.record().value())),
                            msg.committableOffset());
                  } else if (ignore(msg.record().value())) {
                    produce = ProducerMessage.passThrough(msg.committableOffset());
                  } else {
                    produce =
                        ProducerMessage.single(
                            new ProducerRecord<>(topic4, msg.record().value()),
                            msg.committableOffset());
                  }
                  return produce;
                })
            .via(Producer.flexiFlow(producerSettings))
            .map(m -> m.passThrough())
            .toMat(Committer.sink(committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #oneToConditional

    Pair<Consumer.Control, CompletionStage<List<ConsumerRecord<String, String>>>> tuple =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic2, topic3, topic4))
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    produceString(topic1, 10, partition0).toCompletableFuture().get(1, TimeUnit.SECONDS);
    sleepSeconds(10, "to make produce happen");
    assertThat(
        control.drainAndShutdown(ec).toCompletableFuture().get(5, TimeUnit.SECONDS),
        is(Done.done()));
    assertThat(
        tuple.first().shutdown().toCompletableFuture().get(5, TimeUnit.SECONDS), is(Done.done()));
    assertThat(tuple.second().toCompletableFuture().get(5, TimeUnit.SECONDS).size(), is(10));
  }

  @Test
  public void shouldSupportBatchingOfOffsetsWithOffsetContext() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic1 = createTopic(1);
    String topic2 = createTopic(2);
    Consumer.DrainingControl<Done> control =
        Consumer.sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic1))
            .grouped(5)
            .map(
                records -> {
                  String key = records.iterator().next().key();
                  String value =
                      records.stream().map(ConsumerRecord::value).collect(Collectors.joining(","));
                  return ProducerMessage.single(new ProducerRecord<>(topic2, key, value));
                })
            .mapContext(ConsumerMessage::createCommittableOffsetBatch)
            .via(Producer.flowWithContext(producerDefaults()))
            .toMat(Committer.sinkWithOffsetContext(committerDefaults()), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    Pair<Consumer.Control, CompletionStage<List<ConsumerRecord<String, String>>>> pair =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic2))
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    Consumer.Control consumerControl = pair.first();
    CompletionStage<List<ConsumerRecord<String, String>>> consumerRecords = pair.second();

    produceString(topic1, 10, partition0).toCompletableFuture().get(5, TimeUnit.SECONDS);
    sleepSeconds(10, "to make produce happen");
    assertThat(
        control.drainAndShutdown(ec).toCompletableFuture().get(5, TimeUnit.SECONDS),
        is(Done.done()));
    assertThat(
        consumerControl.shutdown().toCompletableFuture().get(5, TimeUnit.SECONDS), is(Done.done()));
    assertThat(consumerRecords.toCompletableFuture().get(5, TimeUnit.SECONDS).size(), is(2));
  }
}
