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

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.javadsl.MetadataClient;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import akka.util.Timeout;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.core.IsInstanceOf;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
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
    final TopicPartition partition0 = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final Set<TopicPartition> partitions = Collections.singleton(partition0);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    final CompletionStage<Map<TopicPartition, Long>> response =
        metadataClient.getBeginningOffsets(partitions);
    final Map<TopicPartition, Long> beginningOffsets = response.toCompletableFuture().join();

    assertThat(beginningOffsets.get(partition0), is(0L));

    metadataClient.stop();
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
    final TopicPartition partition0 = new TopicPartition(topic1, 0);
    final ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(group1);
    final MetadataClient metadataClient =
        MetadataClient.create(consumerSettings, timeout, sys, executor);

    final CompletionStage<Long> response =
        metadataClient.getBeginningOffsetForPartition(partition0);
    final Long beginningOffset = response.toCompletableFuture().join();

    assertThat(beginningOffset, is(0L));

    metadataClient.stop();
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
