/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples.javadsl;


import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

abstract class ConsumerExample {
  protected final ActorSystem system = ActorSystem.create("example");

  protected final Materializer materializer = ActorMaterializer.create(system);

  protected final int maxPartitions = 100;

  protected <T> Flow<T, T, NotUsed> business() {
    return Flow.create();
  }

  protected final ConsumerSettings<byte[], String> consumerSettings =
      ConsumerSettings.create(system, new ByteArrayDeserializer(), new StringDeserializer())
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

  protected final ProducerSettings<byte[], String> producerSettings =
      ProducerSettings.create(system, new ByteArraySerializer(), new StringSerializer())
    .withBootstrapServers("localhost:9092");

  static class DB {
    private AtomicLong offset = new AtomicLong();
        
    public CompletionStage<Done> save(ConsumerRecord<byte[], String> record) {
      System.out.println("DB.save: " + record.value());
      offset.set(record.offset());
      return CompletableFuture.completedFuture(Done.getInstance());
    }

    public CompletionStage<Long> loadOffset() {
      return CompletableFuture.completedFuture(offset.get());
    }

    public CompletionStage<Done> update(String data) {
      System.out.println("DB.update: " + data);
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }

  static class Rocket {
    public CompletionStage<Done> launch(String destination) {
      System.out.println("Rocket launched to " + destination);
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }
}

// Consume messages and store a representation, including offset, in DB
class ExternalOffsetStorageExample extends ConsumerExample {
  public static void main(String[] args) {
    new ExternalOffsetStorageExample().demo();
  }
  
  public void demo() {
    final DB db = new DB();

    db.loadOffset().thenAccept(fromOffset -> {
      Consumer.plainSource(
        consumerSettings,
        Subscriptions.assignmentWithOffset(new TopicPartition("topic1", 0), fromOffset)
      ).mapAsync(1, record -> db.save(record))
      .runWith(Sink.ignore(), materializer);
    });
  }
}

// Consume messages at-most-once
class AtMostOnceExample extends ConsumerExample {
  public static void main(String[] args) {
    new AtMostOnceExample().demo();
  }
  
  public void demo() {
    final Rocket rocket = new Rocket();

    Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
      .mapAsync(1, record -> rocket.launch(record.value()))
      .runWith(Sink.ignore(), materializer);
  }
}

// Consume messages at-least-once
class AtLeastOnceExample extends ConsumerExample {
  public static void main(String[] args) {
    new AtLeastOnceExample().demo();
  }
  
  public void demo() {
    final DB db = new DB();

    Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .mapAsync(1, msg -> db.update(msg.record().value())
        .thenCompose(done -> msg.committableOffset().commitJavadsl()))
      .runWith(Sink.ignore(), materializer);
  }
}

// Consume messages at-least-once, and commit in batches
class AtLeastOnceWithBatchCommitExample extends ConsumerExample {
  public static void main(String[] args) {
    new AtLeastOnceWithBatchCommitExample().demo();
  }
  
  public void demo() {
    final DB db = new DB();

    Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .mapAsync(1, msg ->
        db.update(msg.record().value()).thenApply(done -> msg.committableOffset()))
      .batch(20,
        first -> ConsumerMessage.emptyCommittableOffsetBatch().updated(first),
        (batch, elem) -> batch.updated(elem))
      .mapAsync(1, c -> c.commitJavadsl())
      .runWith(Sink.ignore(), materializer);
  }
}

// Connect a Consumer to Producer
class ConsumerToProducerSinkExample extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerToProducerSinkExample().demo();
  }
  
  public void demo() {
    Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map(msg ->
        new ProducerMessage.Message<byte[], String, ConsumerMessage.Committable>(
            new ProducerRecord<>("topic2", msg.record().value()), msg.committableOffset()))
      .runWith(Producer.commitableSink(producerSettings), materializer);
  }
}

// Connect a Consumer to Producer
class ConsumerToProducerFlowExample extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerToProducerFlowExample().demo();
  }
  
  public void demo() {
    Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map(msg ->
        new ProducerMessage.Message<byte[], String, ConsumerMessage.Committable>(
          new ProducerRecord<>("topic2", msg.record().value()), msg.committableOffset()))
      .via(Producer.flow(producerSettings))
      .mapAsync(producerSettings.parallelism(), result ->
        result.message().passThrough().commitJavadsl())
      .runWith(Sink.ignore(), materializer);
  }
}

// Connect a Consumer to Producer, and commit in batches
class ConsumerToProducerWithBatchCommitsExample extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerToProducerWithBatchCommitsExample().demo();
  }
  
  public void demo() {
    Source<ConsumerMessage.CommittableOffset, Consumer.Control> source =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map(msg ->
          new ProducerMessage.Message<byte[], String, ConsumerMessage.CommittableOffset>(
              new ProducerRecord<>("topic2", msg.record().value()), msg.committableOffset()))
      .via(Producer.flow(producerSettings))
      .map(result -> result.message().passThrough());

      source.batch(20,
          first -> ConsumerMessage.emptyCommittableOffsetBatch().updated(first),
          (batch, elem) -> batch.updated(elem))
        .mapAsync(1, c -> c.commitJavadsl())
        .runWith(Sink.ignore(), materializer);
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
      .map(msg ->
          new ProducerMessage.Message<byte[], String, ConsumerMessage.CommittableOffset>(
              new ProducerRecord<>("topic2", msg.record().value()), msg.committableOffset()))
      .via(Producer.flow(producerSettings))
      .map(result -> result.message().passThrough());

      source
        .groupedWithin(20, Duration.create(5, TimeUnit.SECONDS))
        .map(group -> foldLeft(group))
        .mapAsync(1, c -> c.commitJavadsl())
        .runWith(Sink.ignore(), materializer);
  }

  private ConsumerMessage.CommittableOffsetBatch foldLeft(List<ConsumerMessage.CommittableOffset> group) {
    ConsumerMessage.CommittableOffsetBatch batch = ConsumerMessage.emptyCommittableOffsetBatch();
    for (ConsumerMessage.CommittableOffset elem: group) {
      batch = batch.updated(elem);
    }
    return batch;
  }
}

// Backpressure per partition with batch commit
class ConsumerWithPerPartitionBackpressure extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerWithPerPartitionBackpressure().demo();
  }
  
  public void demo() {
    Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .flatMapMerge(maxPartitions, Pair::second)
      .via(business())
      .batch(
          100,
          first -> ConsumerMessage.emptyCommittableOffsetBatch().updated(first.committableOffset()),
          (batch, elem) -> batch.updated(elem.committableOffset())
      )
      .mapAsync(1, x -> x.commitJavadsl())
      .runWith(Sink.ignore(), materializer);
  }
}

class ExternallyControlledKafkaConsumer extends ConsumerExample {
  public static void main(String[] args) {
    new ExternallyControlledKafkaConsumer().demo();
  }
  
  public void demo() {
    ActorRef consumer = system.actorOf((KafkaConsumerActor.props(consumerSettings)));

    //Manually assign topic partition to it
    Consumer
      .plainExternalSource(consumer, Subscriptions.assignment(new TopicPartition("topic1", 1)))
      .via(business())
      .runWith(Sink.ignore(), materializer);

    //Manually assign another topic partition
    Consumer
      .plainExternalSource(consumer, Subscriptions.assignment(new TopicPartition("topic1", 2)))
      .via(business())
      .runWith(Sink.ignore(), materializer);
  }
}

class ConsumerWithIndependentFlowsPerPartition extends ConsumerExample {
  public static void main(String[] args) {
    new ConsumerWithIndependentFlowsPerPartition().demo();
  }
  
  public void demo() {
    Consumer.Control c = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .map(pair -> pair
        .second()
        .via(business())
        .toMat(Sink.ignore(), Keep.both())
        .run(materializer)
      )
      .mapAsyncUnordered(maxPartitions, (pair) -> pair.second())
      .to(Sink.ignore())
      .run(materializer);
  }
}
