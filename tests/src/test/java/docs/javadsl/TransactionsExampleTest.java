/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import static org.junit.Assert.assertEquals;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Transactional;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.kafka.tests.javadsl.LogCapturingJunit4;
import akka.stream.RestartSettings;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

public class TransactionsExampleTest extends TestcontainersKafkaJunit4Test {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final ActorSystem system = ActorSystem.create("TransactionsExampleTest");
  private final ExecutorService ec = Executors.newSingleThreadExecutor();
  private final ProducerSettings<String, String> producerSettings = txProducerDefaults();

  public TransactionsExampleTest() {
    super(system);
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

  /** Overridden to set a different default timeout for [[#resultOf]]. Default is 5 seconds. */
  protected Duration resultOfTimeout() {
    return Duration.ofSeconds(15);
  }

  @Test
  public void sourceSink() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String sourceTopic = createTopic(1);
    String targetTopic = createTopic(2);
    String transactionalId = createTransactionalId();
    // #transactionalSink
    Consumer.DrainingControl<Done> control =
        Transactional.source(consumerSettings, Subscriptions.topics(sourceTopic))
            .via(business())
            .map(
                msg ->
                    ProducerMessage.single(
                        new ProducerRecord<>(targetTopic, msg.record().key(), msg.record().value()),
                        msg.partitionOffset()))
            .toMat(
                Transactional.sink(producerSettings, transactionalId),
                Consumer::createDrainingControl)
            .run(system);

    // ...

    // #transactionalSink
    String testConsumerGroup = createGroupId(2);
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(probeConsumerSettings(testConsumerGroup), targetTopic, 10);
    produceString(sourceTopic, 10, partition0);
    assertDone(consumer.isShutdown());
    // #transactionalSink
    control.drainAndShutdown(ec);
    // #transactionalSink
    assertDone(control.isShutdown());
    assertEquals(10, resultOf(consumer.drainAndShutdown(ec)).size());
  }

  @Test
  public void withOffsetContext() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String sourceTopic = createTopic(1);
    String targetTopic = createTopic(2);
    String transactionalId = createTransactionalId();
    Consumer.DrainingControl<Done> control =
        Transactional.sourceWithOffsetContext(consumerSettings, Subscriptions.topics(sourceTopic))
            .via(business())
            .map(
                record ->
                    ProducerMessage.single(
                        new ProducerRecord<>(targetTopic, record.key(), record.value())))
            .toMat(
                Transactional.sinkWithOffsetContext(producerSettings, transactionalId),
                Consumer::createDrainingControl)
            .run(system);

    String testConsumerGroup = createGroupId(2);
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(probeConsumerSettings(testConsumerGroup), targetTopic, 10);
    produceString(sourceTopic, 10, partition0);
    assertDone(consumer.isShutdown());
    control.drainAndShutdown(ec);
    assertDone(control.isShutdown());
    assertEquals(10, resultOf(consumer.drainAndShutdown(ec)).size());
  }

  @Test
  public void usingRestartSource() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String sourceTopic = createTopic(1);
    String targetTopic = createTopic(2);
    String transactionalId = createTransactionalId();
    // #transactionalFailureRetry
    AtomicReference<Consumer.Control> innerControl =
        new AtomicReference<>(Consumer.createNoopControl());

    Source<ProducerMessage.Results<String, String, ConsumerMessage.PartitionOffset>, NotUsed>
        stream =
            RestartSource.onFailuresWithBackoff(
                RestartSettings.create(
                    java.time.Duration.ofSeconds(3), // min backoff
                    java.time.Duration.ofSeconds(30), // max backoff
                    0.2), // adds 20% "noise" to vary the intervals slightly
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

    CompletionStage<Done> streamCompletion = stream.runWith(Sink.ignore(), system);

    // Add shutdown hook to respond to SIGTERM and gracefully shutdown stream
    Runtime.getRuntime().addShutdownHook(new Thread(() -> innerControl.get().shutdown()));
    // #transactionalFailureRetry
    String testConsumerGroup = createGroupId(2);
    int messages = 10;
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(probeConsumerSettings(testConsumerGroup), targetTopic, messages);
    assertDone(produceString(sourceTopic, messages, partition0));
    assertDone(consumer.isShutdown());
    assertDone(innerControl.get().shutdown());
    assertEquals(messages, resultOf(consumer.drainAndShutdown(ec)).size());
    assertDone(streamCompletion);
  }

  //  @Test
  //  public void partitionedSourceSink() throws Exception {
  //    ConsumerSettings<String, String> consumerSettings =
  //        consumerDefaults().withGroupId(createGroupId(1));
  //    String sourceTopic = createTopic(1, 2, 1);
  //    String targetTopic = createTopic(2, 1, 1);
  //    String transactionalId = createTransactionalId(1);
  //    // #partitionedTransactionalSink
  //    Consumer.DrainingControl<Done> control =
  //        Transactional.partitionedSource(consumerSettings, Subscriptions.topics(sourceTopic))
  //            .mapAsync(
  //                8,
  //                pair -> {
  //                  Source<ConsumerMessage.TransactionalMessage<String, String>, NotUsed> source =
  //                      pair.second();
  //                  return source
  //                      .via(business())
  //                      .map(
  //                          msg ->
  //                              ProducerMessage.single(
  //                                  new ProducerRecord<>(
  //                                      targetTopic, msg.record().key(), msg.record().value()),
  //                                  msg.partitionOffset()))
  //                      .runWith(Transactional.sink(producerSettings, transactionalId),
  // materializer);
  //                })
  //            .toMat(Sink.ignore(), Keep.both())
  //            .mapMaterializedValue(Consumer::createDrainingControl)
  //            .run(materializer);
  //
  //    // ...
  //
  //    // #partitionedTransactionalSink
  //    String testConsumerGroup = createGroupId(2);
  //    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
  //        consumeString(probeConsumerSettings(testConsumerGroup), targetTopic, 10);
  //    produceString(sourceTopic, 10, partition0);
  //    assertDone(consumer.isShutdown());
  //    // #partitionedTransactionalSink
  //    control.drainAndShutdown(ec);
  //    // #partitionedTransactionalSink
  //    assertDone(control.isShutdown());
  //    assertEquals(10, resultOf(consumer.drainAndShutdown(ec)).size());
  //  }

  protected Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumeString(
      ConsumerSettings<String, String> settings, String topic, long take) {
    return Consumer.plainSource(settings, Subscriptions.topics(topic))
        .take(take)
        .toMat(Sink.seq(), Consumer::createDrainingControl)
        .run(system);
  }

  public ConsumerSettings<String, String> probeConsumerSettings(String groupId) {
    return TransactionsOps$.MODULE$.withProbeConsumerSettings(this.consumerDefaults(), groupId);
  }

  @Override
  public ProducerSettings<String, String> producerDefaults() {
    return TransactionsOps$.MODULE$.withTestProducerSettings(super.producerDefaults());
  }

  public ProducerSettings<String, String> txProducerDefaults() {
    return TransactionsOps$.MODULE$.withTransactionalProducerSettings(super.producerDefaults());
  }
}
