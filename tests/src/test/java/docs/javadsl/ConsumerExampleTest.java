/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.javadsl.Committer;
import akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class ConsumerExampleTest extends EmbeddedKafkaJunit4Test {

  private static final ActorSystem system = ActorSystem.create("ConsumerExampleTest");
  private static final Materializer materializer = ActorMaterializer.create(system);
  private static final Executor ec = Executors.newSingleThreadExecutor();

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
    return "localhost:" + kafkaPort();
  }

  @Override
  public int kafkaPort() {
    return KafkaPorts.ConsumerExamplesTest();
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

  // #settings
  final Config config = system.settings().config().getConfig("akka.kafka.consumer");
  final ConsumerSettings<String, byte[]> consumerSettings =
      ConsumerSettings.create(config, new StringDeserializer(), new ByteArrayDeserializer())
          .withBootstrapServers("localhost:9092")
          .withGroupId("group1")
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
  // #settings

  final ConsumerSettings<String, byte[]> consumerSettingsWithAutoCommit =
      // #settings-autocommit
      consumerSettings
          .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
          .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
  // #settings-autocommit

  @Test
  public void plainSourceWithExternalOffsetStorage() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 1, 1);
    // #plainSource
    final OffsetStorage db = new OffsetStorage();

    CompletionStage<Consumer.Control> controlCompletionStage =
        db.loadOffset()
            .thenApply(
                fromOffset ->
                    Consumer.plainSource(
                            consumerSettings,
                            Subscriptions.assignmentWithOffset(
                                new TopicPartition(topic, /* partition: */ 0), fromOffset))
                        .mapAsync(1, db::businessLogicAndStoreOffset)
                        .to(Sink.ignore())
                        .run(materializer));
    // #plainSource
    assertDone(produceString(topic, 10, partition0()));
    while (db.offsetStore.get() < 9L) {
      sleepMillis(100, "until offsets have increased");
    }
    assertDone(resultOf(controlCompletionStage).shutdown());
  }

  // #plainSource

  class OffsetStorage {
    // #plainSource
    private final AtomicLong offsetStore = new AtomicLong(0L);

    // #plainSource
    public CompletionStage<Done> businessLogicAndStoreOffset(
        ConsumerRecord<String, String> record) { // ... }
      // #plainSource
      offsetStore.set(record.offset());
      return CompletableFuture.completedFuture(Done.getInstance());
    }

    // #plainSource
    public CompletionStage<Long> loadOffset() { // ... }
      // #plainSource

      return CompletableFuture.completedFuture(offsetStore.get());
    }

    public CompletionStage<Done> storeProcessedOffset(long offset) { // ... }
      offsetStore.set(offset);
      return CompletableFuture.completedFuture(Done.getInstance());
    }

    // #plainSource
  }
  // #plainSource

  @Test
  public void atMostOnce() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 1, 1);
    // #atMostOnce
    Consumer.Control control =
        Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics(topic))
            .mapAsync(10, record -> business(record.key(), record.value()))
            .to(Sink.foreach(it -> System.out.println("Done with " + it)))
            .run(materializer);

    // #atMostOnce
    assertDone(produceString(topic, 10, partition0()));
    assertDone(control.shutdown());
  }

  // #atMostOnce #atLeastOnce
  CompletionStage<String> business(String key, String value) { // .... }
    // #atMostOnce #atLeastOnce
    return CompletableFuture.completedFuture("");
  }

  @Test
  public void atLeastOnce() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    CommitterSettings committerSettings = committerDefaults();
    String topic = createTopic(1, 1, 1);
    // #atLeastOnce
    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .thenApply(done -> msg.committableOffset()))
            .toMat(Committer.sink(committerSettings.withMaxBatch(1)), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    // #atLeastOnce
    assertDone(produceString(topic, 10, partition0()));
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void atLeastOnceWithCommitterSink() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 1, 1);
    Config config = system.settings().config().getConfig(CommitterSettings.configPath());
    // #committerSink
    CommitterSettings committerSettings = CommitterSettings.create(config);

    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .<ConsumerMessage.Committable>thenApply(done -> msg.committableOffset()))
            .toMat(Committer.sink(committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #committerSink
    assertDone(produceString(topic, 10, partition0()));
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void commitWithMetadata() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    CommitterSettings committerSettings = committerDefaults();
    String topic = createTopic(1, 1, 1);
    // #commitWithMetadata
    Consumer.DrainingControl<Done> control =
        Consumer.commitWithMetadataSource(
                consumerSettings,
                Subscriptions.topics(topic),
                (record) -> Long.toString(record.timestamp()))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .thenApply(done -> msg.committableOffset()))
            .toMat(Committer.sink(committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #commitWithMetadata
    assertDone(produceString(topic, 10, partition0()));
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void consumerToProducer() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    ProducerSettings<String, String> producerSettings = producerDefaults();
    CommitterSettings committerSettings = committerDefaults();
    String topic1 = createTopic(1, 1, 1);
    String topic2 = createTopic(2, 1, 1);
    String targetTopic = createTopic(10, 1, 1);
    // #consumerToProducerSink
    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1, topic2))
            .map(
                msg ->
                    ProducerMessage.<String, String, ConsumerMessage.Committable>single(
                        new ProducerRecord<>(targetTopic, msg.record().key(), msg.record().value()),
                        msg.committableOffset()))
            .toMat(Producer.committableSink(producerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #consumerToProducerSink
    assertDone(produceString(topic1, 10, partition0()));
    assertDone(produceString(topic2, 10, partition0()));
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(targetTopic, 20);
    assertDone(consumer.isShutdown());
    assertEquals(20, resultOf(consumer.drainAndShutdown(ec)).size());
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void consumerToProducerFlow() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    ProducerSettings<String, String> producerSettings = producerDefaults();
    CommitterSettings committerSettings = committerDefaults();
    String topic = createTopic(1, 1, 1);
    String targetTopic = createTopic(20, 1, 1);
    // #consumerToProducerFlow
    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
            .map(
                msg ->
                    ProducerMessage.single(
                        new ProducerRecord<>(targetTopic, msg.record().key(), msg.record().value()),
                        msg.committableOffset() // the passThrough
                        ))
            .via(Producer.flexiFlow(producerSettings))
            .map(m -> m.passThrough())
            .toMat(Committer.sink(committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #consumerToProducerFlow
    assertDone(produceString(topic, 10, partition0()));
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(targetTopic, 10);
    assertDone(consumer.isShutdown());
    assertEquals(10, resultOf(consumer.drainAndShutdown(ec)).size());
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void committableParitionedSource() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1)).withStopTimeout(Duration.ofMillis(10));
    CommitterSettings committerSettings = committerDefaults();
    int maxPartitions = 2;
    String topic = createTopic(1, maxPartitions, 1);
    // #committablePartitionedSource
    Consumer.DrainingControl<Done> control =
        Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics(topic))
            .flatMapMerge(maxPartitions, Pair::second)
            .via(business())
            .map(msg -> msg.committableOffset())
            .toMat(Committer.sink(committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #committablePartitionedSource
    assertDone(produceString(topic, 10, partition0()));
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void streamPerPartition() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1)).withStopTimeout(Duration.ofMillis(10));
    CommitterSettings committerSettings = committerDefaults();
    int maxPartitions = 2;
    String topic = createTopic(1, maxPartitions, 1);
    // #committablePartitionedSource-stream-per-partition
    Consumer.DrainingControl<Done> control =
        Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics(topic))
            .mapAsyncUnordered(
                maxPartitions,
                pair -> {
                  Source<ConsumerMessage.CommittableMessage<String, String>, NotUsed> source =
                      pair.second();
                  return source
                      .via(business())
                      .map(message -> message.committableOffset())
                      .runWith(Committer.sink(committerSettings), materializer);
                })
            .toMat(Sink.ignore(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #committablePartitionedSource-stream-per-partition
    assertDone(produceString(topic, 10, partition0()));
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void consumerActor() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    ActorRef self = system.deadLetters();
    String topic = createTopic(1, 2, 1);
    int partition0 = 0;
    int partition1 = 1;
    // #consumerActor
    // Consumer is represented by actor
    ActorRef consumer = system.actorOf((KafkaConsumerActor.props(consumerSettings)));

    // Manually assign topic partition to it
    Consumer.Control controlPartition1 =
        Consumer.plainExternalSource(
                consumer, Subscriptions.assignment(new TopicPartition(topic, partition0)))
            .via(business())
            .to(Sink.ignore())
            .run(materializer);

    // Manually assign another topic partition
    Consumer.Control controlPartition2 =
        Consumer.plainExternalSource(
                consumer, Subscriptions.assignment(new TopicPartition(topic, partition1)))
            .via(business())
            .to(Sink.ignore())
            .run(materializer);

    // #consumerActor
    CompletionStage<Done> producePartition0 = produceString(topic, 10, partition0);
    CompletionStage<Done> producePartition1 = produceString(topic, 10, partition1);
    assertDone(producePartition0);
    assertDone(producePartition1);
    assertDone(controlPartition1.shutdown());
    assertDone(controlPartition2.shutdown());
    // #consumerActor

    consumer.tell(KafkaConsumerActor.stop(), self);
    // #consumerActor
  }

  @Test
  public void restartSource() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 2, 1);
    // #restartSource
    AtomicReference<Consumer.Control> control = new AtomicReference<>(Consumer.createNoopControl());

    RestartSource.onFailuresWithBackoff(
            java.time.Duration.ofSeconds(3),
            java.time.Duration.ofSeconds(30),
            0.2,
            () ->
                Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
                    .mapMaterializedValue(
                        c -> {
                          control.set(c);
                          return c;
                        })
                    .via(business()))
        .runWith(Sink.ignore(), materializer);

    // #restartSource
    assertDone(produceString(topic, 10, partition0()));
    // #restartSource
    control.get().shutdown();
    // #restartSource
  }

  // #withRebalanceListenerActor
  static class RebalanceListener extends AbstractLoggingActor {

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              TopicPartitionsAssigned.class,
              assigned -> {
                log().info("Assigned: {}", assigned);
              })
          .match(
              TopicPartitionsRevoked.class,
              revoked -> {
                log().info("Revoked: {}", revoked);
              })
          .build();
    }
  }

  // #withRebalanceListenerActor

  @Test
  public void withRebalanceListener() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 1, 1);
    int messageCount = 10;
    // #withRebalanceListenerActor
    ActorRef rebalanceListener = system.actorOf(Props.create(RebalanceListener.class));

    Subscription subscription =
        Subscriptions.topics(topic)
            // additionally, pass the actor reference:
            .withRebalanceListener(rebalanceListener);

    // use the subscription as usual:
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control =
        Consumer.plainSource(consumerSettings, subscription)
            // #withRebalanceListenerActor
            .take(messageCount)
            // #withRebalanceListenerActor
            .toMat(Sink.seq(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #withRebalanceListenerActor
    assertDone(produceString(topic, messageCount, partition0()));
    assertDone(control.isShutdown());
    assertEquals(messageCount, resultOf(control.drainAndShutdown(ec)).size());
  }

  @Test
  public void consumerMetrics() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 1, 1);
    // #consumerMetrics
    // run the stream to obtain the materialized Control value
    Consumer.DrainingControl<Done> control =
        Consumer.plainSource(
                consumerSettings, Subscriptions.assignment(new TopicPartition(topic, 0)))
            .via(business())
            .toMat(Sink.ignore(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    // #consumerMetrics
    sleepMillis(
        100,
        "to let the control establish itself (fails with `java.lang.IllegalStateException: not yet initialized: only setHandler is allowed in GraphStageLogic constructor)` otherwise");
    // #consumerMetrics
    CompletionStage<Map<MetricName, Metric>> metrics = control.getMetrics();
    metrics.thenAccept(map -> System.out.println("Metrics: " + map));
    // #consumerMetrics
    assertDone(control.drainAndShutdown(ec));
  }

  @Test
  public void shutdownPlainSource() {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 1, 1);
    // #shutdownPlainSource
    final OffsetStorage db = new OffsetStorage();

    CompletionStage<Consumer.DrainingControl<Done>> control =
        db.loadOffset()
            .thenApply(
                fromOffset ->
                    Consumer.plainSource(
                            consumerSettings,
                            Subscriptions.assignmentWithOffset(
                                new TopicPartition(topic, 0), fromOffset))
                        .mapAsync(
                            10,
                            record ->
                                business(record.key(), record.value())
                                    .thenApply(res -> db.storeProcessedOffset(record.offset())))
                        .toMat(Sink.ignore(), Keep.both())
                        .mapMaterializedValue(Consumer::createDrainingControl)
                        .run(materializer));

    // Shutdown the consumer when desired
    control.thenAccept(c -> c.drainAndShutdown(ec));
    // #shutdownPlainSource
  }

  @Test
  public void shutdownCommittable() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(1));
    String topic = createTopic(1, 1, 1);
    CommitterSettings committerSettings = committerDefaults();
    // TODO there is a problem with combining `take` and committing.
    int messageCount = 1;
    // #shutdownCommittableSource
    final Executor ec = Executors.newCachedThreadPool();

    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(
                consumerSettings.withStopTimeout(Duration.ZERO), Subscriptions.topics(topic))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .thenApply(done -> msg.committableOffset()))
            // #shutdownCommittableSource
            .take(messageCount)
            // #shutdownCommittableSource
            .toMat(Committer.sink(committerSettings.withMaxBatch(1)), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    // #shutdownCommittableSource
    assertDone(produceString(topic, messageCount, partition0()));
    assertDone(control.isShutdown());
    assertEquals(Done.done(), resultOf(control.drainAndShutdown(ec), Duration.ofSeconds(20)));
    // #shutdownCommittableSource
    control.drainAndShutdown(ec);
    // #shutdownCommittableSource
  }
}
