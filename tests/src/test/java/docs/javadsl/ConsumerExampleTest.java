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
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.javadsl.PartitionAssignmentHandler;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
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
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.hamcrest.CoreMatchers.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerExampleTest extends TestcontainersKafkaTest {

  private static final ActorSystem system = ActorSystem.create("ConsumerExampleTest");
  private static final Materializer materializer = ActorMaterializer.create(system);
  private static final Executor executor = Executors.newSingleThreadExecutor();

  ConsumerExampleTest() {
    super(system, materializer);
  }

  @AfterAll
  void afterClass() {
    TestKit.shutdownActorSystem(system);
  }

  void assertDone(CompletionStage<Done> stage) throws Exception {
    assertEquals(Done.done(), resultOf(stage));
  }

  private <T> Flow<T, T, NotUsed> business() {
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
  void plainSourceWithExternalOffsetStorage() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
    // #plainSource
    final OffsetStorage db = new OffsetStorage();

    CompletionStage<Consumer.Control> controlCompletionStage =
        db.loadOffset()
            .thenApply(
                fromOffset ->
                    Consumer.plainSource(
                            consumerSettings,
                            Subscriptions.assignmentWithOffset(
                                new TopicPartition(topic, partition0), fromOffset))
                        .mapAsync(1, db::businessLogicAndStoreOffset)
                        .to(Sink.ignore())
                        .run(materializer));
    // #plainSource
    assertDone(produceString(topic, 10, partition0));
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
  void configInheritance() throws Exception {
    // #config-inheritance
    Config config = system.settings().config().getConfig("our-kafka-consumer");
    ConsumerSettings<String, String> consumerSettings =
        ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer());
    // #config-inheritance
    assertEquals("kafka-host:9092", consumerSettings.getProperty("bootstrap.servers"));
  }

  @Test
  void atMostOnce() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
    // #atMostOnce
    Consumer.Control control =
        Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics(topic))
            .mapAsync(10, record -> business(record.key(), record.value()))
            .to(Sink.foreach(it -> System.out.println("Done with " + it)))
            .run(materializer);

    // #atMostOnce
    assertDone(produceString(topic, 10, partition0));
    assertDone(control.shutdown());
  }

  // #atMostOnce #atLeastOnce
  CompletionStage<String> business(String key, String value) { // .... }
    // #atMostOnce #atLeastOnce
    return CompletableFuture.completedFuture("");
  }

  @Test
  void atLeastOnce() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    CommitterSettings committerSettings = committerDefaults();
    String topic = createTopic();
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
    assertDone(produceString(topic, 10, partition0));
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void atLeastOnceWithCommitterSink() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
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
    assertDone(produceString(topic, 10, partition0));
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void commitWithMetadata() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    CommitterSettings committerSettings = committerDefaults();
    String topic = createTopic();
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
    assertDone(produceString(topic, 10, partition0));
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void consumerToProducer() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    ProducerSettings<String, String> producerSettings = producerDefaults();
    CommitterSettings committerSettings = committerDefaults();
    String topic1 = createTopic(1);
    String topic2 = createTopic(2);
    String targetTopic = createTopic(10);
    // #consumerToProducerSink
    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1, topic2))
            .map(
                msg ->
                    ProducerMessage.<String, String, ConsumerMessage.Committable>single(
                        new ProducerRecord<>(targetTopic, msg.record().key(), msg.record().value()),
                        msg.committableOffset()))
            .toMat(Producer.committableSink(producerSettings, committerSettings), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #consumerToProducerSink
    assertDone(produceString(topic1, 10, partition0));
    assertDone(produceString(topic2, 10, partition0));
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(targetTopic, 20);
    assertDone(consumer.isShutdown());
    assertEquals(20, resultOf(consumer.drainAndShutdown(executor)).size());
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void consumerToProducerWithContext() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    ProducerSettings<String, String> producerSettings = producerDefaults();
    CommitterSettings committerSettings = committerDefaults();
    String topic1 = createTopic(1);
    String topic2 = createTopic(2);
    String targetTopic = createTopic(10);
    // #consumerToProducerWithContext
    Consumer.DrainingControl<Done> control =
        Consumer.sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic1, topic2))
            .map(
                record ->
                    ProducerMessage.single(
                        new ProducerRecord<>(targetTopic, record.key(), record.value())))
            .toMat(
                Producer.committableSinkWithOffsetContext(producerSettings, committerSettings),
                Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #consumerToProducerWithContext
    assertDone(produceString(topic1, 10, partition0));
    assertDone(produceString(topic2, 10, partition0));
    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumer =
        consumeString(targetTopic, 20);
    assertDone(consumer.isShutdown());
    assertEquals(20, resultOf(consumer.drainAndShutdown(executor)).size());
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void committablePartitionedSource() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId()).withStopTimeout(Duration.ofMillis(10));
    CommitterSettings committerSettings = committerDefaults();
    int maxPartitions = 2;
    String topic = createTopic(1, maxPartitions);
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
    assertDone(produceString(topic, 10, partition0));
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void streamPerPartition() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId()).withStopTimeout(Duration.ofMillis(10));
    CommitterSettings committerSettings = committerDefaults();
    int maxPartitions = 2;
    String topic = createTopic(1, maxPartitions);
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
    assertDone(produceString(topic, 10, partition0));
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void consumerActor() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    ActorRef self = system.deadLetters();
    String topic = createTopic(1, 2);
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
  void restartSource() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic(1, 2);
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
    assertDone(produceString(topic, 10, partition0));
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
  void withRebalanceListener() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
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
    assertDone(produceString(topic, messageCount, partition0));
    assertDone(control.isShutdown());
    assertEquals(messageCount, resultOf(control.drainAndShutdown(executor)).size());
  }

  @Test
  void withPartitionAssignmentHandler() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
    TopicPartition tp = new TopicPartition(topic, 0);
    int messageCount = 10;
    AtomicReference<Set<TopicPartition>> revoked = new AtomicReference<>();
    AtomicReference<Set<TopicPartition>> assigned = new AtomicReference<>();
    AtomicReference<Set<TopicPartition>> stopped = new AtomicReference<>();

    PartitionAssignmentHandler handler =
        new PartitionAssignmentHandler() {
          public void onRevoke(Set<TopicPartition> revokedTps, RestrictedConsumer consumer) {
            revoked.set(revokedTps);
          }

          public void onAssign(Set<TopicPartition> assignedTps, RestrictedConsumer consumer) {
            assigned.set(assignedTps);
          }

          public void onStop(Set<TopicPartition> currentTps, RestrictedConsumer consumer) {
            stopped.set(currentTps);
          }
        };
    Subscription subscription = Subscriptions.topics(topic).withPartitionAssignmentHandler(handler);

    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control =
        Consumer.plainSource(consumerSettings, subscription)
            .take(messageCount)
            .toMat(Sink.seq(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    assertDone(produceString(topic, messageCount, partition0));
    assertDone(control.isShutdown());
    assertEquals(messageCount, resultOf(control.drainAndShutdown(executor)).size());

    assertThat(revoked.get(), is(Collections.emptySet()));
    assertThat(assigned.get(), hasItem(tp));
    assertThat(stopped.get(), hasItem(tp));
  }

  @Test
  void consumerMetrics() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
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
    assertDone(control.drainAndShutdown(executor));
  }

  @Test
  void shutdownPlainSource() {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
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
    control.thenAccept(c -> c.drainAndShutdown(executor));
    // #shutdownPlainSource
  }

  @Test
  void shutdownCommittable() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
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
    assertDone(produceString(topic, messageCount, partition0));
    assertDone(control.isShutdown());
    assertEquals(Done.done(), resultOf(control.drainAndShutdown(ec), Duration.ofSeconds(20)));
    // #shutdownCommittableSource
    control.drainAndShutdown(ec);
    // #shutdownCommittableSource
  }
}
