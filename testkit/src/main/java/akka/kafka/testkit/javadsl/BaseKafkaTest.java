/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.testkit.internal.KafkaTestKitChecks;
import akka.kafka.testkit.internal.KafkaTestKitClass;
import akka.kafka.testkit.internal.KafkaTestKit;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.functionConverterImpls.FromJavaPredicate;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
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

  /**
   * Overwrite to set different default timeout for
   * [[resultOf[T](stage:java\.util\.concurrent\.CompletionStage[T])* resultOf]].
   */
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
            consumerDefaults().withGroupId(createGroupId(1)).withStopTimeout(Duration.ZERO),
            Subscriptions.topics(topic))
        .take(take)
        .toMat(Sink.seq(), Keep.both())
        .mapMaterializedValue(Consumer::createDrainingControl)
        .run(materializer);
  }

  /**
   * Periodically checks if a given predicate on cluster state holds.
   *
   * <p>If the predicate does not hold after configured amount of time, throws an exception.
   */
  public void waitUntilCluster(Predicate<DescribeClusterResult> predicate) {
    KafkaTestKitChecks.waitUntilCluster(
        settings().clusterTimeout(),
        settings().checkInterval(),
        adminClient(),
        new FromJavaPredicate(predicate),
        log());
  }

  /**
   * Periodically checks if the given predicate on consumer group state holds.
   *
   * <p>If the predicate does not hold after configured amount of time, throws an exception.
   */
  public void waitUntilConsumerGroup(
      String groupId, Predicate<ConsumerGroupDescription> predicate) {
    KafkaTestKitChecks.waitUntilConsumerGroup(
        groupId,
        settings().consumerGroupTimeout(),
        settings().checkInterval(),
        adminClient(),
        new FromJavaPredicate(predicate),
        log());
  }

  /**
   * Periodically checks if the given predicate on consumer summary holds.
   *
   * <p>If the predicate does not hold after configured amount of time, throws an exception.
   */
  public void waitUntilConsumerSummary(
      String groupId, Predicate<Collection<MemberDescription>> predicate) {
    waitUntilConsumerGroup(
        groupId,
        group -> {
          try {
            return group.state() == ConsumerGroupState.STABLE && predicate.test(group.members());
          } catch (Exception ex) {
            return false;
          }
        });
  }

  protected <T> T resultOf(CompletionStage<T> stage) throws Exception {
    return resultOf(stage, resultOfTimeout());
  }

  protected <T> T resultOf(CompletionStage<T> stage, Duration timeout) throws Exception {
    return stage.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
