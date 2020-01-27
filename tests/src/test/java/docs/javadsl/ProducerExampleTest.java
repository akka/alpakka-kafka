/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
// #testkit
import akka.kafka.testkit.javadsl.EmbeddedKafkaTest;
// #testkit
import akka.kafka.tests.javadsl.LogCapturingExtension;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
// #testkit
import akka.testkit.javadsl.TestKit;
// #testkit
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
// #testkit
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
// #testkit

import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

// #testkit

@TestInstance(Lifecycle.PER_CLASS)
// #testkit
@ExtendWith(LogCapturingExtension.class)
// #testkit
class ProducerExampleTest extends EmbeddedKafkaTest {

  private static final ActorSystem system = ActorSystem.create("ProducerExampleTest");
  private static final Materializer materializer = ActorMaterializer.create(system);
  // #testkit

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final ProducerSettings<String, String> producerSettings = producerDefaults();

  // #testkit

  ProducerExampleTest() {
    super(system, materializer, KafkaPorts.ProducerExamplesTest());
  }

  @AfterAll
  void shutdownActorSystem() {
    TestKit.shutdownActorSystem(system);
    executor.shutdown();
  }

  // #testkit
  @Test
  void plainSink() throws Exception {
    String topic = createTopic();
    CompletionStage<Done> done =
        Source.range(1, 100)
            .map(number -> number.toString())
            .map(value -> new ProducerRecord<String, String>(topic, value))
            .runWith(Producer.plainSink(producerSettings), materializer);

    Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control =
        consumeString(topic, 100);
    assertEquals(Done.done(), resultOf(done));
    assertEquals(Done.done(), resultOf(control.isShutdown()));
    CompletionStage<List<ConsumerRecord<String, String>>> result =
        control.drainAndShutdown(executor);
    assertEquals(100, resultOf(result).size());
  }

  // #testkit
}
// #testkit
