/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
// #withTypedRebalanceListenerActor
// #consumerActorTyped
// adds support for actors to a classic actor system and context
import akka.actor.typed.javadsl.Adapter;
// #consumerActorTyped
// #withTypedRebalanceListenerActor
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.javadsl.PartitionAssignmentHandler;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.kafka.tests.javadsl.LogCapturingExtension;
import akka.stream.RestartSettings;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;
import static org.junit.Assert.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(LogCapturingExtension.class)
class ConsumerExampleTest extends TestcontainersKafkaTest {

  private static final ActorSystem system = ActorSystem.create("ConsumerExampleTest");
  private static final Executor executor = Executors.newSingleThreadExecutor();

  ConsumerExampleTest() {
    super(system);
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
                        .run(system));
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
            .run(system);

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
            .toMat(
                Committer.sink(committerSettings.withMaxBatch(1)), Consumer::createDrainingControl)
            .run(system);

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
            .toMat(Committer.sink(committerSettings), Consumer::createDrainingControl)
            .run(system);
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
            .toMat(Committer.sink(committerSettings), Consumer::createDrainingControl)
            .run(system);
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
            .toMat(
                Producer.committableSink(producerSettings, committerSettings),
                Consumer::createDrainingControl)
            .run(system);
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
                Consumer::createDrainingControl)
            .run(system);
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
            .toMat(Committer.sink(committerSettings), Consumer::createDrainingControl)
            .run(system);
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
                      .runWith(Committer.sink(committerSettings), system);
                })
            .toMat(Sink.ignore(), Consumer::createDrainingControl)
            .run(system);
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
    Behaviors.setup(
        ctx -> {
          // #consumerActorTyped

          // Consumer is represented by actor
          ActorRef consumer = Adapter.actorOf(ctx, KafkaConsumerActor.props(consumerSettings));
          // #consumerActorTyped
          return Behaviors.empty();
        });

    // #consumerActor
    // Consumer is represented by actor
    ActorRef consumer = system.actorOf((KafkaConsumerActor.props(consumerSettings)));

    // Manually assign topic partition to it
    Consumer.Control controlPartition1 =
        Consumer.plainExternalSource(
                consumer, Subscriptions.assignment(new TopicPartition(topic, partition0)))
            .via(business())
            .to(Sink.ignore())
            .run(system);

    // Manually assign another topic partition
    Consumer.Control controlPartition2 =
        Consumer.plainExternalSource(
                consumer, Subscriptions.assignment(new TopicPartition(topic, partition1)))
            .via(business())
            .to(Sink.ignore())
            .run(system);

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

    RestartSettings restartSettings =
        RestartSettings.create(Duration.ofSeconds(3), Duration.ofSeconds(30), 0.2);
    CompletionStage<Done> streamCompletion =
        RestartSource.onFailuresWithBackoff(
                restartSettings,
                () ->
                    Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
                        .mapMaterializedValue(
                            c -> {
                              // this is a hack to get access to the Consumer.Control
                              // instances of the latest Kafka Consumer source
                              control.set(c);
                              return c;
                            })
                        .via(business()))
            .runWith(Sink.ignore(), system);

    // #restartSource
    assertDone(produceString(topic, 10, partition0));
    // #restartSource
    control.get().drainAndShutdown(streamCompletion, system.getDispatcher());
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
            .toMat(Sink.seq(), Consumer::createDrainingControl)
            .run(system);
    // #withRebalanceListenerActor
    assertDone(produceString(topic, messageCount, partition0));
    assertDone(control.isShutdown());
    assertEquals(messageCount, resultOf(control.drainAndShutdown(executor)).size());
  }

  @Test
  void withTypedRebalanceListener() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    String topic = createTopic();
    int messageCount = 10;

    // #withTypedRebalanceListenerActor

    Function<ActorContext<ConsumerRebalanceEvent>, Behavior<ConsumerRebalanceEvent>>
        rebalanceListener =
            (ActorContext<ConsumerRebalanceEvent> context) ->
                Behaviors.receive(ConsumerRebalanceEvent.class)
                    .onMessage(
                        TopicPartitionsAssigned.class,
                        assigned -> {
                          context.getLog().info("Assigned: {}", assigned);
                          return Behaviors.same();
                        })
                    .onMessage(
                        TopicPartitionsRevoked.class,
                        revoked -> {
                          context.getLog().info("Revoked: {}", revoked);
                          return Behaviors.same();
                        })
                    .build();
    // #withTypedRebalanceListenerActor

    Behavior<Object> guardian =
        Behaviors.setup(
            guardianCtx -> {
              // #withTypedRebalanceListenerActor

              Behavior<ConsumerRebalanceEvent> listener =
                  Behaviors.setup(ctx -> rebalanceListener.apply(ctx));

              akka.actor.typed.ActorRef<ConsumerRebalanceEvent> typedRef =
                  guardianCtx.spawn(listener, "rebalance-listener");

              akka.actor.ActorRef classicRef = Adapter.toClassic(typedRef);

              Subscription subscription =
                  Subscriptions.topics(topic)
                      // additionally, pass the actor reference:
                      .withRebalanceListener(classicRef);

              Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control =
                  // use the subscription as usual:
                  Consumer.plainSource(consumerSettings, subscription)
                      // #withTypedRebalanceListenerActor
                      .take(messageCount)
                      // #withTypedRebalanceListenerActor
                      .toMat(Sink.seq(), Consumer::createDrainingControl)
                      .run(system);
              // #withTypedRebalanceListenerActor

              assertDone(produceString(topic, messageCount, partition0));
              assertDone(control.isShutdown());
              assertEquals(messageCount, resultOf(control.drainAndShutdown(executor)).size());

              return Behaviors.stopped();
            });

    akka.actor.typed.ActorSystem<Object> typed =
        akka.actor.typed.ActorSystem.create(guardian, "typed-rebalance-listener-example");
    assertDone(typed.getWhenTerminated());
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

    // #partitionAssignmentHandler
    PartitionAssignmentHandler assignmentHandler =
        new PartitionAssignmentHandler() {
          public void onRevoke(Set<TopicPartition> revokedTps, RestrictedConsumer consumer) {
            // #partitionAssignmentHandler
            revoked.set(revokedTps);
            // #partitionAssignmentHandler
          }

          public void onAssign(Set<TopicPartition> assignedTps, RestrictedConsumer consumer) {
            // #partitionAssignmentHandler
            assigned.set(assignedTps);
            // #partitionAssignmentHandler
          }

          public void onLost(Set<TopicPartition> lostTps, RestrictedConsumer consumer) {}

          public void onStop(Set<TopicPartition> currentTps, RestrictedConsumer consumer) {
            // #partitionAssignmentHandler
            stopped.set(currentTps);
            // #partitionAssignmentHandler
          }
        };

    Subscription subscription =
        Subscriptions.topics(topic).withPartitionAssignmentHandler(assignmentHandler);
    // #partitionAssignmentHandler

    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control =
        Consumer.plainSource(consumerSettings, subscription)
            .take(messageCount)
            .toMat(Sink.seq(), Consumer::createDrainingControl)
            .run(system);
    assertDone(produceString(topic, messageCount, partition0));
    assertDone(control.isShutdown());
    assertEquals(messageCount, resultOf(control.drainAndShutdown(executor)).size());

    assertThat(assigned.get(), hasItem(tp));
    assertThat(stopped.get(), hasItem(tp));
    assertThat(revoked.get(), hasItem(tp)); // revoke of partitions occurs after stop
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
            .toMat(Sink.ignore(), Consumer::createDrainingControl)
            .run(system);

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
                        .toMat(Sink.ignore(), Consumer::createDrainingControl)
                        .run(system));

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
            .toMat(
                Committer.sink(committerSettings.withMaxBatch(1)), Consumer::createDrainingControl)
            .run(system);

    // #shutdownCommittableSource
    assertDone(produceString(topic, messageCount, partition0));
    assertDone(control.isShutdown());
    assertEquals(Done.done(), resultOf(control.drainAndShutdown(ec), Duration.ofSeconds(20)));
    // #shutdownCommittableSource
    control.drainAndShutdown(ec);
    // #shutdownCommittableSource
  }
}
