/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

// #metadata
import akka.actor.ActorRef;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.KafkaPorts;
import akka.kafka.Metadata;
import akka.pattern.PatternsCS;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.kafka.common.PartitionInfo;

// #metadata
import akka.actor.ActorSystem;
import java.util.concurrent.TimeUnit;
import akka.kafka.testkit.javadsl.EmbeddedKafkaTest;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class FetchMetadataTest extends EmbeddedKafkaTest {

  private static final ActorSystem sys = ActorSystem.create("FetchMetadataTest");
  private static final Materializer mat = ActorMaterializer.create(sys);
  private static final int kafkaPort = KafkaPorts.FetchMetadataTest();

  @Override
  public ActorSystem system() {
    return sys;
  }

  @Override
  public String bootstrapServers() {
    return "localhost:" + kafkaPort;
  }

  @BeforeClass
  public static void beforeClass() {
    startEmbeddedKafka(kafkaPort, 1);
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

  @Test
  public void demo() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId(0));
    // #metadata
    Duration timeout = Duration.ofSeconds(2);
    ConsumerSettings<String, String> settings =
        consumerSettings.withMetadataRequestTimeout(timeout);

    ActorRef consumer = system().actorOf((KafkaConsumerActor.props(settings)));

    CompletionStage<Metadata.Topics> topicsStage =
        PatternsCS.ask(consumer, Metadata.createListTopics(), timeout)
            .thenApply(reply -> ((Metadata.Topics) reply));

    // convert response
    CompletionStage<Optional<List<String>>> response =
        topicsStage
            .thenApply(Metadata.Topics::getResponse)
            .thenApply(
                responseOptional ->
                    responseOptional.map(
                        map ->
                            map.entrySet()
                                .stream()
                                .flatMap(
                                    entry -> {
                                      String topic = entry.getKey();
                                      List<PartitionInfo> partitionInfos = entry.getValue();
                                      return partitionInfos
                                          .stream()
                                          .map(info -> topic + ": " + info.toString());
                                    })
                                .collect(Collectors.toList())));

    // #metadata
    Optional<List<String>> optionalStrings =
        response.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertTrue(optionalStrings.isPresent());
  }
}
