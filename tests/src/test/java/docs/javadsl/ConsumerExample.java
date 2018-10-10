/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
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
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

abstract class ConsumerExample {
  protected final ActorSystem system = ActorSystem.create("example");

  protected final Materializer materializer = ActorMaterializer.create(system);

  protected final int maxPartitions = 100;

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

  protected final ProducerSettings<String, byte[]> producerSettings =
      ProducerSettings.create(system, new StringSerializer(), new ByteArraySerializer())
          .withBootstrapServers("localhost:9092");
}

// Consume messages and store a representation, including offset, in OffsetStorage
class ExternalOffsetStorageExample extends ConsumerExample {
  public static void main(String[] args) {
    new ExternalOffsetStorageExample().demo();
  }

  public void demo() {
    // #plainSource
    final OffsetStorage db = new OffsetStorage();

    CompletionStage<Consumer.Control> controlCompletionStage =
        db.loadOffset()
            .thenApply(
                fromOffset -> {
                  return Consumer.plainSource(
                          consumerSettings,
                          Subscriptions.assignmentWithOffset(
                              new TopicPartition("topic1", /* partition: */ 0), fromOffset))
                      .mapAsync(1, db::businessLogicAndStoreOffset)
                      .to(Sink.ignore())
                      .run(materializer);
                });
    // #plainSource
  }

  // #plainSource

  class OffsetStorage {
    // #plainSource
    private final AtomicLong offsetStore = new AtomicLong();

    // #plainSource
    public CompletionStage<Done> businessLogicAndStoreOffset(
        ConsumerRecord<String, byte[]> record) { // ... }
      // #plainSource
      offsetStore.set(record.offset());
      return CompletableFuture.completedFuture(Done.getInstance());
    }

    // #plainSource
    public CompletionStage<Long> loadOffset() { // ... }
      // #plainSource

      return CompletableFuture.completedFuture(offsetStore.get());
    }

    // #plainSource
  }
  // #plainSource

}

// Consume messages at-most-once
class AtMostOnceExample extends ConsumerExample {
  public static void main(String[] args) {
    new AtMostOnceExample().demo();
  }

  public void demo() {
    // #atMostOnce
    Consumer.Control control =
        Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
            .mapAsync(10, record -> business(record.key(), record.value()))
            .to(Sink.foreach(it -> System.out.println("Done with " + it)))
            .run(materializer);

    // #atMostOnce
  }

  // #atMostOnce
  CompletionStage<String> business(String key, byte[] value) { // .... }
    // #atMostOnce
    return CompletableFuture.completedFuture("");
  }
}

// Consume messages at-least-once
class AtLeastOnceExample extends ConsumerExample {
  public static void main(String[] args) {
    new AtLeastOnceExample().demo();
  }

  public void demo() {
    // #atLeastOnce
    Consumer.Control control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .thenApply(done -> msg.committableOffset()))
            .mapAsync(1, offset -> offset.commitJavadsl())
            .to(Sink.ignore())
            .run(materializer);
    // #atLeastOnce
  }

  // #atLeastOnce

  CompletionStage<String> business(String key, byte[] value) { // .... }
    // #atLeastOnce
    return CompletableFuture.completedFuture("");
  }
}

// Consume messages at-least-once, and commit in batches
class AtLeastOnceWithBatchCommitExample extends ConsumerExample {
  public static void main(String[] args) {
    new AtLeastOnceWithBatchCommitExample().demo();
  }

  public void demo() {
    // #atLeastOnceBatch
    Consumer.Control control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .thenApply(done -> msg.committableOffset()))
            .batch(
                20,
                ConsumerMessage::createCommittableOffsetBatch,
                ConsumerMessage.CommittableOffsetBatch::updated)
            .mapAsync(3, c -> c.commitJavadsl())
            .to(Sink.ignore())
            .run(materializer);
    // #atLeastOnceBatch
  }

  CompletionStage<String> business(String key, byte[] value) { // .... }
    return CompletableFuture.completedFuture("");
  }
}

// Consume messages at-least-once, and commit in batches with metadata
class CommitWithMetadataExample extends ConsumerExample {
  public static void main(String[] args) {
    new CommitWithMetadataExample().demo();
  }

  public void demo() {
    // #commitWithMetadata
    Consumer.Control control =
        Consumer.commitWithMetadataSource(
                consumerSettings,
                Subscriptions.topics("topic1"),
                (record) -> Long.toString(record.timestamp()))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .thenApply(done -> msg.committableOffset()))
            .batch(
                20,
                ConsumerMessage::createCommittableOffsetBatch,
                ConsumerMessage.CommittableOffsetBatch::updated)
            .mapAsync(3, c -> c.commitJavadsl())
            .to(Sink.ignore())
            .run(materializer);
    // #commitWithMetadata
  }

  CompletionStage<String> business(String key, byte[] value) { // .... }
    return CompletableFuture.completedFuture("");
  }
}

// Connect a Consumer to Producer
class ConsumerToProducerSinkExample extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerToProducerSinkExample().demo();
  }

  public void demo() {
    // #consumerToProducerSink
    Consumer.Control control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1", "topic2"))
            .map(
                msg ->
                    new ProducerMessage.Message<String, byte[], ConsumerMessage.Committable>(
                        new ProducerRecord<>(
                            "targetTopic", msg.record().key(), msg.record().value()),
                        msg.committableOffset()))
            .to(Producer.commitableSink(producerSettings))
            .run(materializer);
    // #consumerToProducerSink
    control.shutdown();
  }
}

// Connect a Consumer to Producer
class ConsumerToProducerFlowExample extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerToProducerFlowExample().demo();
  }

  public void demo() {
    // #consumerToProducerFlow
    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .map(
                msg -> {
                  ProducerMessage.Envelope<String, byte[], ConsumerMessage.Committable> prodMsg =
                      new ProducerMessage.Message<>(
                          new ProducerRecord<>("topic2", msg.record().value()),
                          msg.committableOffset() // the passThrough
                          );
                  return prodMsg;
                })
            .via(Producer.flexiFlow(producerSettings))
            .mapAsync(
                producerSettings.parallelism(),
                result -> {
                  ConsumerMessage.Committable committable = result.passThrough();
                  return committable.commitJavadsl();
                })
            .toMat(Sink.ignore(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #consumerToProducerFlow
  }
}

// Connect a Consumer to Producer, and commit in batches
class ConsumerToProducerWithBatchCommitsExample extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerToProducerWithBatchCommitsExample().demo();
  }

  public void demo() {
    // #consumerToProducerFlowBatch
    Source<ConsumerMessage.CommittableOffset, Consumer.Control> source =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .map(
                msg -> {
                  ProducerMessage.Envelope<String, byte[], ConsumerMessage.CommittableOffset>
                      prodMsg =
                          new ProducerMessage.Message<>(
                              new ProducerRecord<>("topic2", msg.record().value()),
                              msg.committableOffset());
                  return prodMsg;
                })
            .via(Producer.flexiFlow(producerSettings))
            .map(result -> result.passThrough());

    source
        .batch(
            20,
            ConsumerMessage::createCommittableOffsetBatch,
            ConsumerMessage.CommittableOffsetBatch::updated)
        .mapAsync(3, c -> c.commitJavadsl())
        .runWith(Sink.ignore(), materializer);
    // #consumerToProducerFlowBatch
  }
}

// Connect a Consumer to Producer, and commit in batches
class ConsumerToProducerWithBatchCommits2Example extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerToProducerWithBatchCommits2Example().demo();
  }

  public void demo() {
    Source<ConsumerMessage.CommittableOffset, Consumer.Control> source =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .map(
                msg -> {
                  ProducerMessage.Envelope<String, byte[], ConsumerMessage.CommittableOffset>
                      prodMsg =
                          new ProducerMessage.Message<>(
                              new ProducerRecord<>("topic2", msg.record().value()),
                              msg.committableOffset());
                  return prodMsg;
                })
            .via(Producer.flexiFlow(producerSettings))
            .map(result -> result.passThrough());

    // #groupedWithin
    source
        .groupedWithin(20, java.time.Duration.of(5, ChronoUnit.SECONDS))
        .map(ConsumerMessage::createCommittableOffsetBatch)
        .mapAsync(3, c -> c.commitJavadsl())
        // #groupedWithin
        .runWith(Sink.ignore(), materializer);
  }
}

// Backpressure per partition with batch commit
class ConsumerWithPerPartitionBackpressure extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerWithPerPartitionBackpressure().demo();
  }

  public void demo() {
    final Executor ec = Executors.newCachedThreadPool();
    // #committablePartitionedSource
    Consumer.DrainingControl<Done> control =
        Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
            .flatMapMerge(maxPartitions, Pair::second)
            .via(business())
            .map(msg -> msg.committableOffset())
            .batch(
                100,
                ConsumerMessage::createCommittableOffsetBatch,
                ConsumerMessage.CommittableOffsetBatch::updated)
            .mapAsync(3, offsets -> offsets.commitJavadsl())
            .toMat(Sink.ignore(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #committablePartitionedSource
    control.drainAndShutdown(ec);
  }
}

class ConsumerWithIndependentFlowsPerPartition extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerWithIndependentFlowsPerPartition().demo();
  }

  public void demo() {
    final Executor ec = Executors.newCachedThreadPool();
    // #committablePartitionedSource-stream-per-partition
    Consumer.DrainingControl<Done> control =
        Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
            .map(
                pair -> {
                  Source<ConsumerMessage.CommittableMessage<String, byte[]>, NotUsed> source =
                      pair.second();
                  return source
                      .via(business())
                      .mapAsync(1, message -> message.committableOffset().commitJavadsl())
                      .runWith(Sink.ignore(), materializer);
                })
            .mapAsyncUnordered(maxPartitions, completion -> completion)
            .toMat(Sink.ignore(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);
    // #committablePartitionedSource-stream-per-partition
    control.drainAndShutdown(ec);
  }
}

class ExternallyControlledKafkaConsumer extends ConsumerExample {
  public static void main(String[] args) {
    new ExternallyControlledKafkaConsumer().demo();
  }

  public void demo() {
    ActorRef self = system.deadLetters();
    // #consumerActor
    // Consumer is represented by actor
    ActorRef consumer = system.actorOf((KafkaConsumerActor.props(consumerSettings)));

    // Manually assign topic partition to it
    Consumer.Control controlPartition1 =
        Consumer.plainExternalSource(
                consumer, Subscriptions.assignment(new TopicPartition("topic1", 1)))
            .via(business())
            .to(Sink.ignore())
            .run(materializer);

    // Manually assign another topic partition
    Consumer.Control controlPartition2 =
        Consumer.plainExternalSource(
                consumer, Subscriptions.assignment(new TopicPartition("topic1", 2)))
            .via(business())
            .to(Sink.ignore())
            .run(materializer);

    consumer.tell(KafkaConsumerActor.stop(), self);
    // #consumerActor
  }
}

class RestartingConsumer extends ConsumerExample {
  public static void main(String[] args) {
    new RestartingConsumer().demo();
  }

  public void demo() {
    // #restartSource
    AtomicReference<Consumer.Control> control = new AtomicReference<>(Consumer.createNoopControl());

    RestartSource.onFailuresWithBackoff(
            java.time.Duration.ofSeconds(3),
            java.time.Duration.ofSeconds(30),
            0.2,
            () ->
                Consumer.plainSource(consumerSettings, Subscriptions.topics("topic1"))
                    .mapMaterializedValue(
                        c -> {
                          control.set(c);
                          return c;
                        })
                    .via(business()))
        .runWith(Sink.ignore(), materializer);

    control.get().shutdown();
    // #restartSource
  }
}

class RebalanceListenerCallbacksExample extends ConsumerExample {
  public static void main(String[] args) {
    new ExternallyControlledKafkaConsumer().demo();
  }

  // #withRebalanceListenerActor
  class RebalanceListener extends AbstractLoggingActor {

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

  public void demo(ActorSystem system) {
    // #withRebalanceListenerActor
    ActorRef rebalanceListener = this.system.actorOf(Props.create(RebalanceListener.class));

    Subscription subscription =
        Subscriptions.topics("topic")
            // additionally, pass the actor reference:
            .withRebalanceListener(rebalanceListener);

    // use the subscription as usual:
    Consumer.plainSource(consumerSettings, subscription);
    // #withRebalanceListenerActor
  }
}

class ConsumerMetricsExample extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerMetricsExample().demo();
  }

  public void demo() {
    // #consumerMetrics
    // run the stream to obtain the materialized Control value
    Consumer.Control control =
        Consumer.plainSource(
                consumerSettings, Subscriptions.assignment(new TopicPartition("topic1", 2)))
            .via(business())
            .to(Sink.ignore())
            .run(materializer);

    CompletionStage<Map<MetricName, Metric>> metrics = control.getMetrics();
    metrics.thenAccept(map -> System.out.println("Metrics: " + map));
    // #consumerMetrics
  }
}

// Shutdown via Consumer.Control
class ShutdownPlainSourceExample extends ConsumerExample {
  public static void main(String[] args) {
    new ExternalOffsetStorageExample().demo();
  }

  public void demo() {
    // #shutdownPlainSource
    final OffsetStorage db = new OffsetStorage();

    db.loadOffset()
        .thenAccept(
            fromOffset -> {
              Consumer.Control control =
                  Consumer.plainSource(
                          consumerSettings,
                          Subscriptions.assignmentWithOffset(
                              new TopicPartition("topic1", 0), fromOffset))
                      .mapAsync(
                          10,
                          record -> {
                            return business(record.key(), record.value())
                                .thenApply(res -> db.storeProcessedOffset(record.offset()));
                          })
                      .toMat(Sink.ignore(), Keep.left())
                      .run(materializer);

              // Shutdown the consumer when desired
              control.shutdown();
            });
    // #shutdownPlainSource
  }

  CompletionStage<String> business(String key, byte[] value) { // .... }
    return CompletableFuture.completedFuture("");
  }

  class OffsetStorage {

    private final AtomicLong offsetStore = new AtomicLong();

    public CompletionStage<Done> storeProcessedOffset(long offset) { // ... }
      offsetStore.set(offset);
      return CompletableFuture.completedFuture(Done.getInstance());
    }

    public CompletionStage<Long> loadOffset() { // ... }
      return CompletableFuture.completedFuture(offsetStore.get());
    }
  }
}

// Shutdown when batching commits
class ShutdownCommittableSourceExample extends ConsumerExample {
  public static void main(String[] args) {
    new AtLeastOnceExample().demo();
  }

  public void demo() {
    // #shutdownCommitableSource
    final Executor ec = Executors.newCachedThreadPool();

    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .mapAsync(
                1,
                msg ->
                    business(msg.record().key(), msg.record().value())
                        .thenApply(done -> msg.committableOffset()))
            .batch(
                20,
                first -> ConsumerMessage.createCommittableOffsetBatch(first),
                (batch, elem) -> batch.updated(elem))
            .mapAsync(3, c -> c.commitJavadsl())
            .toMat(Sink.ignore(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    control.drainAndShutdown(ec);
    // #shutdownCommitableSource
  }

  CompletionStage<String> business(String key, byte[] value) { // .... }
    return CompletableFuture.completedFuture("");
  }
}
