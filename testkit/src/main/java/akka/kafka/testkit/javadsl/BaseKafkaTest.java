/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.testkit.internal.KafkaTestKitClass;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public abstract class BaseKafkaTest extends KafkaTestKitClass {

  public static final int partition0 = 0;

  public final Logger log = LoggerFactory.getLogger(getClass());

  protected final Materializer materializer;

  protected BaseKafkaTest(ActorSystem system, Materializer materializer, String bootstrapServers) {
    super(system, bootstrapServers);
    this.materializer = materializer;
  }

  @Override
  public Logger log() {
    return log;
  }

  /** Overwrite to set different default timeout for [[#resultOf]]. */
  protected Duration resultOfTimeout() {
    return Duration.ofSeconds(5);
  }

  protected CompletionStage<Done> produceString(String topic, int messageCount, int partition) {
    return Source.fromIterator(() -> IntStream.range(0, messageCount).iterator())
        .map(Object::toString)
        .map(n -> new ProducerRecord<String, String>(topic, partition, DefaultKey(), n))
        .runWith(Producer.plainSink(producerDefaults()), materializer);
  }

  protected Consumer.DrainingControl<List<ConsumerRecord<String, String>>> consumeString(
      String topic, long take) {
    return Consumer.plainSource(
            consumerDefaults().withGroupId(createGroupId(1)), Subscriptions.topics(topic))
        .take(take)
        .toMat(Sink.seq(), Keep.both())
        .mapMaterializedValue(Consumer::createDrainingControl)
        .run(materializer);
  }

  protected <T> T resultOf(CompletionStage<T> stage) throws Exception {
    return resultOf(stage, resultOfTimeout());
  }

  protected <T> T resultOf(CompletionStage<T> stage, Duration timeout) throws Exception {
    return stage.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
