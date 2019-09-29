/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.javadsl.MetadataClient;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import akka.util.Timeout;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.core.IsInstanceOf;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;

public class MetadataClientTest extends TestcontainersKafkaJunit4Test {

  private static final ActorSystem sys = ActorSystem.create("MetadataClientTest");
  private static final Materializer mat = ActorMaterializer.create(sys);
  private static final Executor ec = Executors.newSingleThreadExecutor();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  public MetadataClientTest() {
    super(sys, mat);
  }

  @Test
  public void shouldFetchBeginningOffsetsForGivenPartitions() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    final TopicPartition partition0 = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Set<TopicPartition> partitions = Collections.singleton(partition0);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    final CompletionStage<Map<TopicPartition, Long>> response =
        MetadataClient.getBeginningOffsets(consumerActor, partitions, timeout, ec);
    final Map<TopicPartition, Long> beginningOffsets = response.toCompletableFuture().join();

    assertThat(beginningOffsets.get(partition0), is(0L));

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());
  }

  @Test
  public void shouldFailInCaseOfAnExceptionDuringFetchBeginningOffsetsForNonExistingTopics() {
    expectedException.expect(CompletionException.class);
    expectedException.expectCause(
        IsInstanceOf.instanceOf(org.apache.kafka.common.errors.InvalidTopicException.class));

    final String group1 = createGroupId();
    final TopicPartition nonExistingPartition = new TopicPartition("non-existing topic", 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Set<TopicPartition> partitions = Collections.singleton(nonExistingPartition);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    final CompletionStage<Map<TopicPartition, Long>> response =
        MetadataClient.getBeginningOffsets(consumerActor, partitions, timeout, ec);

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());

    response.toCompletableFuture().join();
  }

  @Test
  public void shouldFetchBeginningOffsetForGivenPartition() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    final TopicPartition partition0 = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    final CompletionStage<Long> response =
        MetadataClient.getBeginningOffsetForPartition(consumerActor, partition0, timeout, ec);
    final Long beginningOffset = response.toCompletableFuture().join();

    assertThat(beginningOffset, is(0L));

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());
  }

  @Test
  public void shouldFailInCaseOfAnExceptionDuringFetchBeginningOffsetForNonExistingTopic() {
    expectedException.expect(CompletionException.class);
    expectedException.expectCause(
        IsInstanceOf.instanceOf(org.apache.kafka.common.errors.InvalidTopicException.class));

    final String group1 = createGroupId();
    final TopicPartition nonExistingPartition = new TopicPartition("non-existing topic", 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    final CompletionStage<Long> response =
        MetadataClient.getBeginningOffsetForPartition(consumerActor, nonExistingPartition, timeout, ec);

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());

    response.toCompletableFuture().join();
  }

  @Test
  public void shouldFetchEndOffsetsForGivenPartitions() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    final TopicPartition partition0 = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Set<TopicPartition> partitions = Collections.singleton(partition0);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    produceString(topic1, 10, partition0.partition()).toCompletableFuture().join();

    final CompletionStage<Map<TopicPartition, Long>> response =
        MetadataClient.getEndOffsets(consumerActor, partitions, timeout, ec);
    final Map<TopicPartition, Long> endOffsets = response.toCompletableFuture().join();

    assertThat(endOffsets.get(partition0), is(10L));

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());
  }

  @Test
  public void shouldFailInCaseOfAnExceptionDuringFetchEndOffsetsForNonExistingTopic() {
    expectedException.expect(CompletionException.class);
    expectedException.expectCause(
        IsInstanceOf.instanceOf(org.apache.kafka.common.errors.InvalidTopicException.class));

    final String group1 = createGroupId();
    final TopicPartition nonExistingPartition = new TopicPartition("non-existing topic", 0);
    final Set<TopicPartition> partitions = Collections.singleton(nonExistingPartition);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    final CompletionStage<Map<TopicPartition, Long>> response =
        MetadataClient.getEndOffsets(consumerActor, partitions, timeout, ec);

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());

    response.toCompletableFuture().join();
  }

  @Test
  public void shouldFetchEndOffsetForGivenPartition() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    final TopicPartition partition0 = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    produceString(topic1, 10, partition0.partition()).toCompletableFuture().join();

    final CompletionStage<Long> response =
        MetadataClient.getEndOffsetForPartition(consumerActor, partition0, timeout, ec);
    final Long endOffset = response.toCompletableFuture().join();

    assertThat(endOffset, is(10L));

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());
  }

  @Test
  public void shouldFailInCaseOfAnExceptionDuringFetchEndOffsetForNonExistingTopic() {
    expectedException.expect(CompletionException.class);
    expectedException.expectCause(
        IsInstanceOf.instanceOf(org.apache.kafka.common.errors.InvalidTopicException.class));

    final String group1 = createGroupId();
    final TopicPartition nonExistingPartition = new TopicPartition("non-existing topic", 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    final CompletionStage<Long> response =
        MetadataClient.getEndOffsetForPartition(consumerActor, nonExistingPartition, timeout, ec);

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());

    response.toCompletableFuture().join();
  }

  @Test
  public void shouldFetchTopicList() {
    final String group = createGroupId();
    final String topic1 = createTopic(1, 2);
    final String topic2 = createTopic(2, 1);
    final ConsumerSettings<String, String> consumerSettings = consumerDefaults().withGroupId(group);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final ActorRef consumerActor = system().actorOf(KafkaConsumerActor.props(consumerSettings));

    produceString(topic1, 10, 0).toCompletableFuture().join();
    produceString(topic1, 10, 1).toCompletableFuture().join();
    produceString(topic2, 10, 0).toCompletableFuture().join();

    final CompletionStage<Map<String, List<PartitionInfo>>> response =
        MetadataClient.getListTopics(consumerActor, timeout, ec);
    final Map<String, List<PartitionInfo>> topics = response.toCompletableFuture().join();

    final Set<Integer> partitionsForTopic1 =
        topics.get(topic1).stream().map(PartitionInfo::partition).collect(toSet());

    final Set<Integer> partitionsForTopic2 =
        topics.get(topic2).stream().map(PartitionInfo::partition).collect(toSet());

    assertThat(partitionsForTopic1, containsInAnyOrder(0, 1));
    assertThat(partitionsForTopic2, containsInAnyOrder(0));

    consumerActor.tell(KafkaConsumerActor.stop(), ActorRef.noSender());
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
