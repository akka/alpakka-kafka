---
project.description: React on Kafka rebalancing the partitions assigned to an Alpakka Kafka consumer.
---
# React on Partition Assignment

Alpakka Kafka allows to react to the Kafka broker's balancing of partitions within a consumer group in two ways:

1. callbacks to the @apidoc[PartitionAssignmentHandler]
1. messages to a @ref[rebalance listener actor](#listening-for-rebalance-events)

## Partition Assignment Handler

Kafka balances partitions between all consumers within a consumer group. When new consumers join or leave the group partitions are revoked from and assigned to those consumers.

@@@ note { title="API may change" }
This @apidoc[PartitionAssignmentHandler] API was introduced in Alpakka Kafka 2.0.0 and may still be subject to change.

Please give input on its usefulness in [Issue #985](https://github.com/akka/alpakka-kafka/issues/985).
@@@

Alpakka Kafka's @apidoc[PartitionAssignmentHandler] expects callbacks to be implemented, all are called with a set of @javadoc[TopicPartition](org.apache.kafka.common.TopicPartition)s and a reference to the @apidoc[RestrictedConsumer] which allows some access to the Kafka @javadoc[Consumer](org.apache.kafka.clients.consumer.Consumer) instance used internally by Alpakka Kafka.

1. `onRevoke` is called when the Kafka broker revokes partitions from this consumer
1. `onAssign` is called when the Kafka broker assigns partitions to this consumer
1. `onLost` is called when partition metadata has changed and partitions no longer exist.  This can occur if a topic is deleted or if the leader's metadata is stale. For details see [KIP-429 Incremental Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol).
1. `onStop` is called when the Alpakka Kafka consumer source is about to stop

Rebalancing starts with revoking partitions from all consumers in a consumer group and assigning all partitions to consumers in a second phase. During rebalance no consumer within that consumer group receives any messages.

The @apidoc[PartitionAssignmentHandler] is Alpakka Kafka's replacement of the Kafka client library's @javadoc[ConsumerRebalanceListener](org.apache.kafka.clients.consumer.ConsumerRebalanceListener).

@@@ warning

All methods on the @apidoc[PartitionAssignmentHandler] are called synchronously during Kafka's poll and rebalance logic. They block any other activity for that consumer.

If any of these methods take longer than the timeout configured in `akka.kafka.consumer.partition-handler-warning` (default 5 seconds) a warning will be logged.

@@@

This example shows an implementation of the `PartitionAssignmentHandler` and how it is passed to the consumer via the `Subscription`.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #partitionAssignmentHandler }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #partitionAssignmentHandler }


## Listening for rebalance events

You may set up an rebalance event listener actor that will be notified when your consumer will be assigned or revoked 
from consuming from specific topic partitions. Two kinds of messages will be sent to this listener actor:

* @apidoc[TopicPartitionsAssigned]
* @apidoc[TopicPartitionsRevoked]

You can use a typed @apidoc[akka.actor.typed.ActorRef] to implement your rebalance event listener by converting it into a classic actor ref.
See the example below and read the @extref[Coexistence](akka:/typed/coexisting.html) page of the Akka Documentation for more details on Akka Classic and Typed interoperability.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #withTypedRebalanceListenerActor }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #withTypedRebalanceListenerActor }

Or with Classic Actors

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #withRebalanceListenerActor }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #withRebalanceListenerActor }
