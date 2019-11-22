/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

/// *
// * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
// * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
// */
//
package docs.javadsl;

// #metadataClient
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.javadsl.MetadataClient;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import akka.util.Timeout;
// #metadataClient
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
  private static final Executor executor = Executors.newSingleThreadExecutor();
  private static final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);

  @Rule public ExpectedException expectedException = ExpectedException.none();

  public MetadataClientTest() {
    super(sys, mat);
  }

  @Test
  public void shouldFetchBeginningOffsetsForGivenPartitions() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    // #metadataClient
    final TopicPartition partition = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Set<TopicPartition> partitions = Collections.singleton(partition);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    final CompletionStage<Map<TopicPartition, Long>> response =
        metadataClient.getBeginningOffsets(partitions);
    final Map<TopicPartition, Long> beginningOffsets = response.toCompletableFuture().join();
    // #metadataClient

    assertThat(beginningOffsets.get(partition), is(0L));

    // #metadataClient
    metadataClient.stop();
    // #metadataClient
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
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    final CompletionStage<Map<TopicPartition, Long>> response =
        metadataClient.getBeginningOffsets(partitions);

    metadataClient.stop();

    response.toCompletableFuture().join();
  }

  @Test
  public void shouldFetchBeginningOffsetForGivenPartition() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    final TopicPartition partition = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    final CompletionStage<Long> response = metadataClient.getBeginningOffsetForPartition(partition);
    final Long beginningOffset = response.toCompletableFuture().join();

    assertThat(beginningOffset, is(0L));

    metadataClient.stop();
  }

  @Test
  public void shouldFetchEndOffsetsForGivenPartitions() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    final TopicPartition partition = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    produceString(topic1, 10, partition.partition()).toCompletableFuture().join();

    final CompletionStage<Map<TopicPartition, Long>> response =
        metadataClient.getEndOffsets(Collections.singleton(partition));
    final Map<TopicPartition, Long> endOffsets = response.toCompletableFuture().join();

    assertThat(endOffsets.get(partition), is(10L));

    metadataClient.stop();
  }

  @Test
  public void shouldFailInCaseOfAnExceptionDuringFetchEndOffsetsForNonExistingTopic() {
    expectedException.expect(CompletionException.class);
    expectedException.expectCause(
        IsInstanceOf.instanceOf(org.apache.kafka.common.errors.InvalidTopicException.class));

    final String group1 = createGroupId();
    final TopicPartition nonExistingPartition = new TopicPartition("non-existing topic", 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    final CompletionStage<Map<TopicPartition, Long>> response =
        metadataClient.getEndOffsets(Collections.singleton(nonExistingPartition));

    metadataClient.stop();
    response.toCompletableFuture().join();
  }

  @Test
  public void shouldFetchEndOffsetForGivenPartition() {
    final String topic1 = createTopic();
    final String group1 = createGroupId();
    final TopicPartition partition = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    produceString(topic1, 10, partition.partition()).toCompletableFuture().join();

    final CompletionStage<Long> response = metadataClient.getEndOffsetForPartition(partition);
    final Long endOffset = response.toCompletableFuture().join();

    assertThat(endOffset, is(10L));
    metadataClient.stop();
  }

  @Test
  public void shouldFetchTopicList() {
    final String group = createGroupId();
    final String topic1 = createTopic(1, 2);
    final String topic2 = createTopic(2, 1);
    final ConsumerSettings<String, String> consumerSettings = consumerDefaults().withGroupId(group);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    produceString(topic1, 10, 0).toCompletableFuture().join();
    produceString(topic1, 10, 1).toCompletableFuture().join();
    produceString(topic2, 10, 0).toCompletableFuture().join();

    final CompletionStage<Map<String, List<PartitionInfo>>> response = metadataClient.listTopics();

    final Map<String, List<PartitionInfo>> topics = response.toCompletableFuture().join();
    final Set<Integer> partitionsForTopic1 =
        topics.get(topic1).stream().map(PartitionInfo::partition).collect(toSet());
    final Set<Integer> partitionsForTopic2 =
        topics.get(topic2).stream().map(PartitionInfo::partition).collect(toSet());

    assertThat(partitionsForTopic1, containsInAnyOrder(0, 1));
    assertThat(partitionsForTopic2, containsInAnyOrder(0));

    metadataClient.stop();
  }

  @Test
  public void shouldFetchPartitionsInfoForGivenTopic() {
    final String group = createGroupId();
    final String topic = createTopic(1, 2);
    final ConsumerSettings<String, String> consumerSettings = consumerDefaults().withGroupId(group);
    final Timeout timeout = new Timeout(1, TimeUnit.SECONDS);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    produceString(topic, 10, 0).toCompletableFuture().join();
    produceString(topic, 10, 1).toCompletableFuture().join();

    final CompletionStage<List<PartitionInfo>> response = metadataClient.getPartitionsFor(topic);

    final List<PartitionInfo> partitionInfos = response.toCompletableFuture().join();
    final Set<Integer> partitions =
        partitionInfos.stream().map(PartitionInfo::partition).collect(toSet());

    assertThat(partitions, containsInAnyOrder(0, 1));

    metadataClient.stop();
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
