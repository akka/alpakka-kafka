/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ProducerMessage;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.tests.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;

// #factories
import akka.kafka.testkit.ConsumerResultFactory;
import akka.kafka.testkit.ProducerResultFactory;
import akka.kafka.testkit.javadsl.ConsumerControlFactory;
// #factories

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class TestkitSamplesTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final ActorSystem sys = ActorSystem.create("TestkitSamplesTest");

  @AfterClass
  public static void afterClass() {
    TestKit.shutdownActorSystem(sys);
  }

  @Test
  public void withoutBrokerTesting() throws Exception {
    String topic = "topic";
    String targetTopic = "target-topic";
    String groupId = "group1";
    long startOffset = 100L;
    int partition = 0;
    CommitterSettings committerSettings = CommitterSettings.create(sys);

    // #factories

    // create elements emitted by the mocked Consumer
    List<ConsumerMessage.CommittableMessage<String, String>> elements =
        Arrays.asList(
            ConsumerResultFactory.committableMessage(
                new ConsumerRecord<>(topic, partition, startOffset, "key", "value 1"),
                ConsumerResultFactory.committableOffset(
                    groupId, topic, partition, startOffset, "metadata")),
            ConsumerResultFactory.committableMessage(
                new ConsumerRecord<>(topic, partition, startOffset + 1, "key", "value 2"),
                ConsumerResultFactory.committableOffset(
                    groupId, topic, partition, startOffset + 1, "metadata 2")));

    // create a source imitating the Consumer.committableSource
    Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control>
        mockedKafkaConsumerSource =
            Source.cycle(elements::iterator)
                .viaMat(ConsumerControlFactory.controlFlow(), Keep.right());

    // create a source imitating the Producer.flexiFlow
    Flow<
            ProducerMessage.Envelope<String, String, ConsumerMessage.CommittableOffset>,
            ProducerMessage.Results<String, String, ConsumerMessage.CommittableOffset>,
            NotUsed>
        mockedKafkaProducerFlow =
            Flow
                .<ProducerMessage.Envelope<String, String, ConsumerMessage.CommittableOffset>>
                    create()
                .map(
                    msg -> {
                      if (msg instanceof ProducerMessage.Message) {
                        ProducerMessage.Message<String, String, ConsumerMessage.CommittableOffset>
                            msg2 =
                                (ProducerMessage.Message<
                                        String, String, ConsumerMessage.CommittableOffset>)
                                    msg;
                        return ProducerResultFactory.result(msg2);
                      } else throw new RuntimeException("unexpected element: " + msg);
                    });

    // run the flow as if it was connected to a Kafka broker
    Pair<Consumer.Control, CompletionStage<Done>> stream =
        mockedKafkaConsumerSource
            .map(
                msg -> {
                  ProducerMessage.Envelope<String, String, ConsumerMessage.CommittableOffset>
                      message =
                          new ProducerMessage.Message<>(
                              new ProducerRecord<>(
                                  targetTopic, msg.record().key(), msg.record().value()),
                              msg.committableOffset());
                  return message;
                })
            .via(mockedKafkaProducerFlow)
            .map(ProducerMessage.Results::passThrough)
            .toMat(Committer.sink(committerSettings), Keep.both())
            .run(sys);
    // #factories

    Thread.sleep(1 * 1000L);
    assertThat(
        stream.first().shutdown().toCompletableFuture().get(2, TimeUnit.SECONDS),
        is(Done.getInstance()));
    assertThat(
        stream.second().toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }
}
