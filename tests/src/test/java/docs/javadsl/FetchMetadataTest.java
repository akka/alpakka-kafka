/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #metadata
import akka.actor.ActorRef;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.Metadata;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.kafka.tests.javadsl.LogCapturingJunit4;
import akka.pattern.Patterns;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.kafka.common.PartitionInfo;

// #metadata
import akka.actor.ActorSystem;
import java.util.concurrent.TimeUnit;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class FetchMetadataTest extends TestcontainersKafkaJunit4Test {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final ActorSystem sys = ActorSystem.create("FetchMetadataTest");

  public FetchMetadataTest() {
    super(sys);
  }

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(sys);
  }

  @Test
  public void demo() throws Exception {
    ConsumerSettings<String, String> consumerSettings =
        consumerDefaults().withGroupId(createGroupId());
    // #metadata
    Duration timeout = Duration.ofSeconds(2);
    ConsumerSettings<String, String> settings =
        consumerSettings.withMetadataRequestTimeout(timeout);

    ActorRef consumer = system().actorOf((KafkaConsumerActor.props(settings)));

    CompletionStage<Metadata.Topics> topicsStage =
        Patterns.ask(consumer, Metadata.createListTopics(), timeout)
            .thenApply(reply -> ((Metadata.Topics) reply));

    // convert response
    CompletionStage<Optional<List<String>>> response =
        topicsStage
            .thenApply(Metadata.Topics::getResponse)
            .thenApply(
                responseOptional ->
                    responseOptional.map(
                        map ->
                            map.entrySet().stream()
                                .flatMap(
                                    entry -> {
                                      String topic = entry.getKey();
                                      List<PartitionInfo> partitionInfos = entry.getValue();
                                      return partitionInfos.stream()
                                          .map(info -> topic + ": " + info.toString());
                                    })
                                .collect(Collectors.toList())));

    // #metadata
    Optional<List<String>> optionalStrings =
        response.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertTrue(optionalStrings.isPresent());
  }
}
