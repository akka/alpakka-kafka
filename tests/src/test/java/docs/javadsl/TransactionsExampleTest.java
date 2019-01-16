/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Transactional;
import akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class TransactionsExampleTest extends EmbeddedKafkaJunit4Test {

  private static final ActorSystem system = ActorSystem.create("TransactionsExampleTest");
  private static final Materializer materializer = ActorMaterializer.create(system);
  private final ExecutorService ec = Executors.newSingleThreadExecutor();
  private final ProducerSettings<String, String> producerSettings = producerDefaults();

  public TransactionsExampleTest() {
    super(system, materializer, KafkaPorts.JavaTransactionsExamples());
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(system);
  }

  protected void assertDone(CompletionStage<Done> stage) throws Exception {
    assertEquals(Done.done(), resultOf(stage));
  }

  protected <T> Flow<T, T, NotUsed> business() {
    return Flow.create();
  }

  @Test
  public void sourceSink() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String sourceTopic = createTopic(1, 1, 1);
    String targetTopic = createTopic(2, 1, 1);
    String transactionalId = createTransactionalId(1);
    // #transactionalSink
    Consumer.DrainingControl<Done> control =
        Transactional.source(consumerSettings, Subscriptions.topics(sourceTopic))
            .via(business())
            .map(
                msg ->
                    ProducerMessage.single(
                        new ProducerRecord<>(targetTopic, msg.record().key(), msg.record().value()),
                        msg.partitionOffset()))
            .toMat(Transactional.sink(producerSettings, transactionalId), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    // ...

    // #transactionalSink
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(targetTopic, 10);
    produceString(sourceTopic, 10, partition0);
    assertDone(consumer.isShutdown());
    // #transactionalSink
    control.drainAndShutdown(ec);
    // #transactionalSink
    assertDone(control.isShutdown());
    assertEquals(10, resultOf(consumer.drainAndShutdown(ec)).size());
  }

  @Test
  public void usingRestartSource() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String sourceTopic = createTopic(1, 1, 1);
    String targetTopic = createTopic(2, 1, 1);
    String transactionalId = createTransactionalId(1);
    // #transactionalFailureRetry
    AtomicReference<Consumer.Control> innerControl =
        new AtomicReference<>(Consumer.createNoopControl());

    Source<ProducerMessage.Results<String, String, ConsumerMessage.PartitionOffset>, NotUsed>
        stream =
            RestartSource.onFailuresWithBackoff(
                java.time.Duration.ofSeconds(3), // min backoff
                java.time.Duration.ofSeconds(30), // max backoff
                0.2, // adds 20% "noise" to vary the intervals slightly
                () ->
                    Transactional.source(consumerSettings, Subscriptions.topics(sourceTopic))
                        .via(business())
                        .map(
                            msg ->
                                ProducerMessage.single(
                                    new ProducerRecord<>(
                                        targetTopic, msg.record().key(), msg.record().value()),
                                    msg.partitionOffset()))
                        // side effect out the `Control` materialized value because it can't be
                        // propagated through the `RestartSource`
                        .mapMaterializedValue(
                            control -> {
                              innerControl.set(control);
                              return control;
                            })
                        .via(Transactional.flow(producerSettings, transactionalId)));

    CompletionStage<Done> streamCompletion = stream.runWith(Sink.ignore(), materializer);

    // Add shutdown hook to respond to SIGTERM and gracefully shutdown stream
    Runtime.getRuntime().addShutdownHook(new Thread(() -> innerControl.get().shutdown()));
    // #transactionalFailureRetry
    int messages = 10;
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(targetTopic, messages);
    assertDone(produceString(sourceTopic, messages, partition0));
    assertDone(consumer.isShutdown());
    assertDone(innerControl.get().shutdown());
    assertEquals(messages, resultOf(consumer.drainAndShutdown(ec)).size());
    assertDone(streamCompletion);
  }
}
