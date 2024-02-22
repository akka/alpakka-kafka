---
project.description: Alpakka Kafka provides a module to use Kafka with Akka Cluster External Sharding.
---
# Akka Cluster Sharding

Akka Cluster allows the user to use an @extref[external shard allocation](akka:/typed/cluster-sharding.html#external-shard-allocation) strategy in order to give the user more control over how many shards are created and what cluster nodes they are assigned to. 
If you consume Kafka messages into your Akka Cluster application then it's possible to run an Alpakka Kafka Consumer on each cluster node and co-locate Kafka partitions with Akka Cluster shards. 
When partitions and shards are co-located together then there is less chance that a message must be transmitted over the network by the Akka Cluster Shard Coordinator to a destination user sharded entity.

## Project Info

@@project-info{ projectId="cluster-sharding" }

## Dependency

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka-cluster-sharding_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2="$akka.version$"
  group2=com.typesafe.akka
  artifact2=akka-cluster-sharding-typed_$scala.binary.version$
  version2=AkkaVersion
}

This module contains an Akka extension called `KafkaClusterSharding` and depends on `akka-cluster-sharding-typed`.

## Setup

There are two steps required to setup the cluster sharding module.

* Initialize Akka Cluster Sharding with a @scaladoc[ShardingMessageExtractor](akka.cluster.sharding.typed.ShardingMessageExtractor) to route Kafka consumed messages to the correct Akka Cluster shard and user entity.
* Use a provided Rebalance Listener in your @scaladoc[ConsumerSettings](akka.kafka.ConsumerSettings) to update the external shard allocation at runtime when Kafka Consumer Group rebalances occur.

@@@ note

A complete example of using this module exists in an [`akka/samples`](https://github.com/akka/akka/tree/main/samples) project called [`akka-sample-kafka-sharding`](https://github.com/akka/akka/tree/main/samples/akka-sample-kafka-to-sharding-scala).  
It's a self-contained example that can run on a developer's laptop.

@@@

## Sharding Message Extractors

To setup the @scaladoc[ShardingMessageExtractor](akka.cluster.sharding.typed.ShardingMessageExtractor) pick a factory method in the `KafkaClusterSharding` Akka extension that best fits your use case. 
This module provides two kinds of extractors, extractors for entities that are within a @scaladoc[ShardingEnvelope](akka.cluster.sharding.typed.ShardingEnvelope) and without.  
They're called `messageExtractor` and `messageExtractorNoEnvelope` respectively.

To route Kafka messages to the correct user entity we must use the same algorithm used to define the Kafka partition for the consumed message. 
This module implements the Murmur2-based hashing algorithm that's used in the Kafka @javadoc[DefaultPartitioner](org.apache.kafka.clients.producer.Partitioner) that's used by default in the Kafka Producer. 
The input to this algorithm is the entity key and the number of partitions used in the topic the message was consumed from. 
Therefore it's critical to use the same Kafka message key (sharded entity id) and number of Kafka topic partitions (shards). 
The message extractors can optionally look up the number of shards given a topic name, or the user can provide the number of shards explicitly.

To get the @scaladoc[ShardingMessageExtractor](akka.cluster.sharding.typed.ShardingMessageExtractor) call the `messageExtractor` overload that's suitable for your use case.  
In the following example we asynchronously request an extractor that does not use a sharding envelope and will use the same number of partitions as the given topic name.

Given a user entity.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/ClusterShardingExample.scala) { #user-entity }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/ClusterShardingExample.java) { #user-entity }

Create a `MessageExtractor`.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/ClusterShardingExample.scala) { #message-extractor }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/ClusterShardingExample.java) { #message-extractor }

Setup Akka Typed Cluster Sharding.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/ClusterShardingExample.scala) { #setup-cluster-sharding }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/ClusterShardingExample.java) { #setup-cluster-sharding }

## Rebalance Listener

The Rebalance Listener is a pre-defined Actor that will handle @scaladoc[ConsumerRebalanceEvents](akka.kafka.ConsumerRebalanceEvent) that will update the Akka Cluster External Sharding strategy when subscribed partitions are re-assigned to consumers running on different cluster nodes. 
This makes sure that shards remain local to Kafka Consumers after a consumer group rebalance.
The Rebalance Listener is returned as a Typed @scaladoc[ActorRef[ConsumerRebalanceEvent]](akka.actor.typed.ActorRef) and must be converted to a classic @scaladoc[ActorRef](akka.actor.ActorRef) before being passed to @scaladoc[ConsumerSettings](akka.kafka.ConsumerSettings).

@@@ note

It's recommended to use the same value for both the Kafka Consumer Group ID and the @scaladoc[EntityTypeKey](akka.cluster.sharding.typed.scaladsl.EntityTypeKey).
This allows you to create multiple Kafka Consumer Groups that consume the same type of messages from the same topic, but are routed to different @scaladoc[Behaviors](akka.actor.typed.Behavior) to be processed in a different way.

For example, a `user-events` topic is consumed by two consumer groups.
One consumer group is used to maintain an up-to-date view of the user's profile and the other is used to represent an aggregate history of the types of user events.
The same message type is used by separate Alpakka Kafka consumers, but the messages are routed to different Akka Cluster Sharding Coordinators that are setup to use separate @scaladoc[Behaviors](akka.actor.typed.Behavior).  

@@@ 

Create the rebalance listener using the extension and pass it into an Alpakka Kafka @scaladoc[Subscription](akka.kafka.Subscription).

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/ClusterShardingExample.scala) { #rebalance-listener }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/ClusterShardingExample.java) { #rebalance-listener }
