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
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MetadataClientTest extends TestcontainersKafkaJunit4Test {

  private static final ActorSystem sys = ActorSystem.create("MetadataClientTest");
  private static final Materializer mat = ActorMaterializer.create(sys);
  private static final Executor ec = Executors.newSingleThreadExecutor();

  public MetadataClientTest() {
    super(sys, mat);
  }

  @Test
  public void shouldFetchBeginningOffsetsForGivenPartitionsTest() {
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
  public void shouldFetchBeginningOffsetForGivenPartitionTest() {
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

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
