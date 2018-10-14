/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.AutoSubscription;
import akka.kafka.KafkaPorts;
import akka.kafka.ManualSubscription;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.EmbeddedKafkaTest;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class AssignmentTest extends EmbeddedKafkaTest {

  private static final ActorSystem sys = ActorSystem.create("AssignmentTest");
  private static final Materializer mat = ActorMaterializer.create(sys);

  @Override
  public ActorSystem system() {
    return sys;
  }

  @Override
  public String bootstrapServers() {
    return "localhost:" + KafkaPorts.AssignmentTest();
  }

  @BeforeClass
  public static void beforeClass() {
    startEmbeddedKafka(KafkaPorts.AssignmentTest(), 1);
  }

  @Test
  public void mustConsumeFromTheSpecifiedTopic() throws ExecutionException, InterruptedException {
    final String topic = createTopic(0, 1, 1);
    final String group = createGroupId(0);
    final Integer totalMessages = 100;
    final CompletionStage<Done> producerCompletion =
        Source.range(1, totalMessages)
            .map(msg -> new ProducerRecord<>(topic, 0, DefaultKey(), msg.toString()))
            .runWith(Producer.plainSink(producerDefaults()), mat);

    producerCompletion.toCompletableFuture().get();

    // #single-topic
    final AutoSubscription subscription = Subscriptions.topics(topic);
    final Source<ConsumerRecord<String, String>, Consumer.Control> consumer =
        Consumer.plainSource(consumerDefaults().withGroupId(group), subscription);
    // #single-topic

    final Integer receivedMessages =
        consumer
            .takeWhile(m -> Integer.valueOf(m.value()) < totalMessages, true)
            .runWith(Sink.seq(), mat)
            .toCompletableFuture()
            .get()
            .size();

    assertEquals(totalMessages, receivedMessages);
  }

  @Test
  public void mustConsumeFromTheSpecifiedPartition()
      throws ExecutionException, InterruptedException {
    final String topic = createTopic(1, 2, 1);
    final Integer totalMessages = 100;
    final CompletionStage<Done> producerCompletion =
        Source.range(1, totalMessages)
            .map(
                msg -> {
                  final Integer partition = msg % 2;
                  return new ProducerRecord<>(topic, partition, DefaultKey(), msg.toString());
                })
            .runWith(Producer.plainSink(producerDefaults()), mat);

    producerCompletion.toCompletableFuture().get();

    // #assingment-single-partition
    final Integer partition = 0;
    final ManualSubscription subscription =
        Subscriptions.assignment(new TopicPartition(topic, partition));
    final Source<ConsumerRecord<String, String>, Consumer.Control> consumer =
        Consumer.plainSource(consumerDefaults(), subscription);
    // #assingment-single-partition

    final CompletionStage<List<Integer>> consumerCompletion =
        consumer
            .take(totalMessages / 2)
            .map(msg -> Integer.valueOf(msg.value()))
            .runWith(Sink.seq(), mat);
    final List<Integer> messages = consumerCompletion.toCompletableFuture().get();
    messages.forEach(m -> assertEquals(0, m % 2));
  }

  @Test
  public void mustConsumeFromTheSpecifiedPartitionAndOffset()
      throws ExecutionException, InterruptedException {
    final String topic = createTopic(2, 1, 1);
    final Integer totalMessages = 100;
    final CompletionStage<Done> producerCompletion =
        Source.range(1, totalMessages)
            .map(msg -> new ProducerRecord<>(topic, 0, DefaultKey(), msg.toString()))
            .runWith(Producer.plainSink(producerDefaults()), mat);

    producerCompletion.toCompletableFuture().get();

    // #assingment-single-partition-offset
    final Integer partition = 0;
    final long offset = totalMessages / 2;
    final ManualSubscription subscription =
        Subscriptions.assignmentWithOffset(new TopicPartition(topic, partition), offset);
    final Source<ConsumerRecord<String, String>, Consumer.Control> consumer =
        Consumer.plainSource(consumerDefaults(), subscription);
    // #assingment-single-partition-offset

    final CompletionStage<List<Long>> consumerCompletion =
        consumer.take(totalMessages / 2).map(ConsumerRecord::offset).runWith(Sink.seq(), mat);
    final List<Long> messages = consumerCompletion.toCompletableFuture().get();
    IntStream.range(0, (int) offset).forEach(idx -> assertEquals(idx, messages.get(idx) - offset));
  }

  @Test
  public void mustConsumeFromTheSpecifiedPartitionAndTimestamp()
      throws ExecutionException, InterruptedException {
    final String topic = createTopic(3, 1, 1);
    final Integer totalMessages = 100;
    final CompletionStage<Done> producerCompletion =
        Source.range(1, totalMessages)
            .map(
                msg ->
                    new ProducerRecord<>(
                        topic, 0, System.currentTimeMillis(), DefaultKey(), msg.toString()))
            .runWith(Producer.plainSink(producerDefaults()), mat);

    producerCompletion.toCompletableFuture().get();

    // #assingment-single-partition-timestamp
    final Integer partition = 0;
    final Long now = System.currentTimeMillis();
    final Long messagesSince = now - 5000;
    final ManualSubscription subscription =
        Subscriptions.assignmentOffsetsForTimes(
            new TopicPartition(topic, partition), messagesSince);
    final Source<ConsumerRecord<String, String>, Consumer.Control> consumer =
        Consumer.plainSource(consumerDefaults(), subscription);
    // #assingment-single-partition-timestamp

    final CompletionStage<List<Long>> consumerCompletion =
        consumer
            .takeWhile(m -> Integer.valueOf(m.value()) < totalMessages, true)
            .map(ConsumerRecord::timestamp)
            .runWith(Sink.seq(), mat);
    final long oldMessages =
        consumerCompletion
            .toCompletableFuture()
            .get()
            .stream()
            .map(t -> t - now)
            .filter(t -> t > 5000)
            .count();
    assertEquals(0, oldMessages);
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
