/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.external.ExternalShardAllocationStrategy;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.kafka.AutoSubscription;
import akka.kafka.ConsumerRebalanceEvent;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.cluster.sharding.KafkaClusterSharding;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.util.Timeout;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class ClusterShardingExample {

  // #user-entity
  static final class User {
    public final String id;
    public final String mame;

    User(String id, String mame) {
      this.id = id;
      this.mame = mame;
    }
  }
  // #user-entity

  public static Behavior<User> userBehaviour() {
    return Behaviors.empty();
  }

  public static <T> Flow<T, T, NotUsed> userBusiness() {
    return Flow.create();
  }

  public static void example() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "ClusterShardingExample");
    String kafkaBootstrapServers = "localhost:9092";

    // #message-extractor
    // automatically retrieving the number of partitions requires a round trip to a Kafka broker
    CompletionStage<KafkaClusterSharding.KafkaShardingNoEnvelopeExtractor<User>> messageExtractor =
        KafkaClusterSharding.get(system)
            .messageExtractorNoEnvelope(
                "user-topic",
                Duration.ofSeconds(10),
                (User msg) -> msg.id,
                ConsumerSettings.create(
                    Adapter.toClassic(system), new StringDeserializer(), new StringDeserializer()));
    // #message-extractor

    // #setup-cluster-sharding

    String groupId = "user-topic-group-id";
    EntityTypeKey<User> typeKey = EntityTypeKey.create(User.class, groupId);

    messageExtractor.thenAccept(
        extractor ->
            ClusterSharding.get(system)
                .init(
                    Entity.of(typeKey, ctx -> userBehaviour())
                        .withAllocationStrategy(
                            new ExternalShardAllocationStrategy(
                                system, typeKey.name(), Timeout.create(Duration.ofSeconds(5))))
                        .withMessageExtractor(extractor)));
    // #setup-cluster-sharding

    // #rebalance-listener
    akka.actor.typed.ActorRef<ConsumerRebalanceEvent> rebalanceListener =
        KafkaClusterSharding.get(system).rebalanceListener(typeKey);

    ConsumerSettings<String, byte[]> consumerSettings =
        ConsumerSettings.create(
                Adapter.toClassic(system), new StringDeserializer(), new ByteArrayDeserializer())
            .withBootstrapServers(kafkaBootstrapServers)
            .withGroupId(
                typeKey
                    .name()); // use the same group id as we used in the `EntityTypeKey` for `User`

    // pass the rebalance listener to the topic subscription
    AutoSubscription subscription =
        Subscriptions.topics("user-topic")
            .withRebalanceListener(Adapter.toClassic(rebalanceListener));

    // run & materialize the stream
    Consumer.plainSource(consumerSettings, subscription)
        .via(userBusiness())
        .runWith(Sink.ignore(), system);
    // #rebalance-listener

  }
}
