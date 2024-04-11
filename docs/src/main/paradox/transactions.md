---
project.description: Alpakka has support for Kafka Transactions which provide guarantees that messages processed in a consume-transform-produce workflow are processed exactly once or not at all.
---
# Transactions

Kafka Transactions provide guarantees that messages processed in a consume-transform-produce workflow (consumed from a source topic, transformed, and produced to a destination topic) are processed exactly once or not at all.  This is achieved through coordination between the Kafka consumer group coordinator, transaction coordinator, and the consumer and producer clients used in the user application. The Kafka producer marks messages that are consumed from the source topic as "committed" only once the transformed messages are successfully produced to the sink.  

For full details on how transactions are achieved in Kafka you may wish to review the Kafka Improvement Proposal [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) and its associated [design document](https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#heading=h.xq0ee1vnpz4o).   

## Transactional Source

The @apidoc[Transactional.source](Transactional$) emits a @apidoc[ConsumerMessage.TransactionalMessage] which contains topic, partition, and offset information required by the producer during the commit process.  Unlike with @apidoc[ConsumerMessage.CommittableMessage], the user is not responsible for committing transactions, this is handled by a @apidoc[Transactional.flow](Transactional$) or @apidoc[Transactional.sink](Transactional$).

This source overrides the Kafka consumer property `isolation.level` to `read_committed`, so that only committed messages can be consumed.

A consumer group ID must be provided.

Only use this source if you have the intention to connect it to a @apidoc[Transactional.flow](Transactional$) or @apidoc[Transactional.sink](Transactional$).

<!-- TODO: uncomment when Transacitonal.partitionedSource is ready
## Transactional Partitioned Source

The @apidoc[Transactional.partitionedSource](Transactional$) is similar to the  `Transactional.source`.
It allows you to run transactional workloads per partition which makes it easier to distribute your transactional application across multiple instances.
When a topic-partition is assigned to a consumer the source will emit a tuple with the assigned topic-partition and a corresponding source.
When a topic-partition is revoked, the corresponding source completes.
 
One of the main advantages of using the `Transactional.partitionedSource` is that the transactional producer will automatically create a new `transactional.id` concatenated from the `transactionalId` provided by the user and the consumer group id, topic, and partition number associated with messages from the source.
This allows users to distribute multiple instances of the application without having to worry about *transactional producer fencing* from conflicting duplicate `transactional.id`'s, which would be the case when using the non-partitioned `Transactional.source`.

@@@note 

The partitioned source requires a Kafka Producer per source (per partition) in order to prevent producer fencing.
This can lead to several performance implications.

1. A single producer per application has the opportunity to collectively batch sends to allow for better throughput.
If we subdivide the same producing workload with multiple producers then we will lose the efficiency of consecutive batching to Kafka that one producer can manage.
Since the Kafka Producer is threadsafe we would ideally only have one Producer per Alpakka Kafka application, but this isn't possible if we want to distribute our transactional application across multiple instances.

2. The Kafka cluster will receive more connection and request overhead because there are more batches sent from more producers.

This is a known issue in the Apache Kafka community and there's a Kafka Improvement Proposal (KIP), [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics), that's been created to address the problem.
Below is an excerpt from its Motivation section.

> The problem we are trying to solve in this proposal is a semantic mismatch between consumers in a group and transactional producers. In a consumer group, ownership of partitions can transfer between group members through the rebalance protocol. For transactional producers, assignments are assumed to be static. Every transactional id must map to a consistent set of input partitions. To preserve the static partition mapping in a consumer group where assignments are frequently changing, the simplest solution is to create a separate producer for every input partition. This is what Kafka Streams does today.

For more details see [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) ([Design Document](https://docs.google.com/document/d/1LhzHGeX7_Lay4xvrEXxfciuDWATjpUXQhrEIkph9qRE/edit)).

@@@

-->

## Transactional Sink and Flow

The @apidoc[Transactional.sink](Transactional$) is similar to the @apidoc[Producer.committableSink](Producer$) in that messages will be automatically committed as part of a transaction.  The @apidoc[Transactional.flow](Transactional$) or @apidoc[Transactional.sink](Transactional$) are required when connecting a consumer to a producer to achieve a transactional workflow.

They override producer properties `enable.idempotence` to `true` and `max.in.flight.requests.per.connection` to `1` as required by the Kafka producer to enable transactions.
The `transaction.timeout.ms` is set to 10s as recommended in [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics). In addition, you can optionally set `akka.kafka.producer.transaction-id-prefix` to prefix in front of the generated transaction ID should your specifications require this level of control.

## Consume-Transform-Produce Workflow

Kafka transactions are handled transparently to the user.  The @apidoc[Transactional.source](Transactional$) will enforce that a consumer group id is specified. All other Kafka consumer and producer properties required to enable transactions are overridden.

Transactions are committed on an interval which can be controlled with the producer config `akka.kafka.producer.eos-commit-interval`, similar to how exactly once works with Kafka Streams.  The default value is `100ms`.  The larger commit interval is the more records will need to be reprocessed in the event of failure and the transaction is aborted.

When the stream is materialized and the producer sees the first message it will initialize a transaction.  Every commit interval (`eos-commit-interval`) we check if there are any offsets available to commit.  If offsets exist then we suspend backpressured demand while we drain all outstanding messages that have not yet been successfully acknowledged (if any) and then commit the transaction.  After the commit succeeds a new transaction is begun and we re-initialize demand for upstream messages.

Messages are also drained from the stream when the consumer gets a rebalance of partitions. In that case, the consumer will wait in the `onPartitionsRevoked` callback until all of the messages have been drained from the stream and the transaction is committed before allowing the rebalance to continue. The amount of total time the consumer will wait for draining is controlled by the `akka.kafka.consumer.commit-timeout`, and the interval between checks is controlled by the `akka.kafka.consumer.eos-draining-check-interval` configuration settings.

To gracefully shutdown the stream and commit the current transaction you must call `shutdown()` on the @apidoc[(javadsl|scaladsl).Consumer.Control] materialized value to await all produced message acknowledgements and commit the final transaction.  

### Simple Example

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/TransactionsExample.scala) { #transactionalSink }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/TransactionsExampleTest.java) { #transactionalSink }


<!-- TODO: uncomment when Transacitonal.partitionedSource is ready
### Partitioned Source Example

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/TransactionsExample.scala) { #partitionedTransactionalSink }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/TransactionsExampleTest.java) { #partitionedTransactionalSink }
-->

### Recovery From Failure

When any stage in the stream fails the whole stream will be torn down.  In the general case it's desirable to allow transient errors to fail the whole stream because they cannot be recovered from within the application.  Transient errors can be caused by network partitions, Kafka broker failures, @javadoc[ProducerFencedException](org.apache.kafka.common.errors.ProducerFencedException)'s from other application instances, and so on.  When the stream encounters transient errors then the current transaction will be aborted before the stream is torn down.  Any produced messages that were not committed will not be available to downstream consumers as long as those consumers are configured with `isolation.level = read_committed`.

For transient errors we can choose to rely on the Kafka producer's configuration to retry, or we can handle it ourselves at the Akka Streams or Application layer.  Using the @extref[RestartSource](akka:/stream/stream-error.html#delayed-restarts-with-a-backoff-stage) we can backoff connection attempts so that we don't hammer the Kafka cluster in a tight loop.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/TransactionsExample.scala) { #transactionalFailureRetry }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/TransactionsExampleTest.java) { #transactionalFailureRetry }

## Caveats

There are several scenarios that this library's implementation of Kafka transactions does not automatically account for.

All of the scenarios covered in the @ref[At-Least-Once Delivery documentation](atleastonce.md) (Multiple Effects per Commit, Non-Sequential Processing, and Conditional Message Processing) are applicable to transactional scenarios as well.

<!-- TODO: replace above with Transactional.partitionedSources is available
When using `Transactional.source` only one application instance per `transactional.id` is allowed.  If two application instances with the same `transactional.id` are run at the same time then the instance that registers with Kafka's transaction coordinator second will throw a @javadoc[ProducerFencedException](org.apache.kafka.common.errors.ProducerFencedException) so it doesn't interfere with transactions in process by the first instance.  To distribute multiple transactional workflows for the same subscription you can use the @ref[Transactional Partitioned Source](#transactional-partitioned-source) `Transactional.partitionedSource`, which manages the `transactional.id` so that no producer fencing occurs.
-->

Any state in the transformation logic is not part of a transaction.  It's left to the user to rebuild state when applying stateful operations with transaction.  It's possible to encode state into messages produced to topics during a transaction.  For example you could produce messages to a topic that represents an event log as part of a transaction.  This event log can be replayed to reconstitute the correct state before the stateful stream resumes consuming again at startup.

Any side effects that occur in the transformation logic is not part of a transaction (i.e. writes to a database).  

The exactly-once-semantics are guaranteed only when your flow consumes from and produces to the same Kafka cluster. Producing to partitions from a 3rd-party source or consuming partitions from one Kafka cluster and producing to another Kafka cluster are not supported.

## Further Reading

For more information on exactly once and transactions in Kafka please consult the following resources.

* [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) ([Design Document](https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#heading=h.xq0ee1vnpz4o))
* [KIP-129: Streams Exactly-Once Semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics) ([Design Document](https://docs.google.com/document/d/1pGZ8xtOOyGwDYgH5vA6h19zOMMaduFK1DAB8_gBYA2c/edit#heading=h.vkrkjfth3p8p))
* [KIP-447: EOS Scalability Design](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) ([Design Document](https://docs.google.com/document/d/1LhzHGeX7_Lay4xvrEXxfciuDWATjpUXQhrEIkph9qRE/edit))
* [You Cannot Have Exactly-Once Delivery Redux](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery-redux/) by Tyler Treat
* [Exactly-once Semantics are Possible: Here’s How Kafka Does it](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
