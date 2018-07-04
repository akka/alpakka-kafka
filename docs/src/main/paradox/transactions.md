# Transactions

Kafka Transactions provide guarantees that messages processed in a consume-transform-produce workflow (consumed from a source topic, transformed, and produced to a destination topic) are processed exactly once or not at all.  This is achieved through coordination between the Kafka consumer group coordinator, transaction coordinator, and the consumer and producer clients used in the user application.  The Kafka producer marks messages that are consumed from the source topic as "committed" only once the transformed messages are successfully produced to the sink.  

For full details on how transactions are achieved in Kafka you may wish to review the Kafka Improvement Proposal [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) and its associated [design document](https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#heading=h.xq0ee1vnpz4o).   

## Transactional Source

The `Transactional.source` emits a `ConsumerMessage.TransactionalMessage` (@scaladoc[API](akka.kafka.ConsumerMessage$$TransactionalMessage)) which contains topic, partition, and offset information required by the producer during the commit process.  Unlike with `ConsumerMessage.CommittableMessage`, the user is not responsible for committing transactions, this is handled by `Transactional.flow` or `Transactional.sink`.

This source overrides the Kafka consumer property `isolation.level` to `read_committed`, so that only committed messages can be consumed.

A consumer group ID must be provided.

Only use this source if you have the intention to connect it to `Transactional.flow` or `Transactional.sink`.

## Transactional Sink and Flow

The `Transactional.sink` is similar to the `Consumer.commitableSink` in that messages will be automatically committed as part of a transaction.  The `Transactional.sink` or `Transactional.flow` are required when connecting a consumer to a producer to achieve a transactional workflow.

They override producer properties `enable.idempotence` to `true` and `max.in.flight.requests.per.connection` to `1` as required by the Kafka producer to enable transactions.

A `transactional.id` must be defined and unique for each instance of the application.

## Consume-Transform-Produce Workflow

Kafka transactions are handled transparently to the user.  The `Transactional.source` will enforce that a consumer group id is specified and the `Transactional.flow` or `Transactional.sink` will enforce that a `transactional.id` is specified.  All other Kafka consumer and producer properties required to enable transactions are overridden.

Transactions are committed on an interval which can be controlled with the producer config `akka.kafka.producer.eos-commit-interval`, similar to how exactly once works with Kafka Streams.  The default value is `100ms`.  The larger commit interval is the more records will need to be reprocessed in the event of failure and the transaction is aborted.

When the stream is materialized the producer will initialize the transaction for the provided `transactional.id` and a transaction will begin.  Every commit interval (`eos-commit-interval`) we check if there are any offsets available to commit.  If offsets exist then we suspend backpressured demand while we drain all outstanding messages that have not yet been successfully acknowledged (if any) and then commit the transaction.  After the commit succeeds a new transaction is begun and we re-initialize demand for upstream messages.

To gracefully shutdown the stream and commit the current transaction you must call `shutdown()` on the `Control` (@scala[@scaladoc[API](akka.kafka.scaladsl.Consumer$$Control)]@java[@scaladoc[API](akka.kafka.javadsl.Consumer$$Control)]) materialized value to await all produced message acknowledgements and commit the final transaction.  

### Simple Example

Scala
: @@ snip [transactionalSink](../../../../tests/src/test/scala/docs//scaladsl/TransactionsExample.scala) { #transactionalSink }

Java
: @@ snip [transactionalSink](../../test/java/sample/javadsl/TransactionsExample.java) { #transactionalSink }

### Recovery From Failure

When any stage in the stream fails the whole stream will be torn down.  In the general case it's desirable to allow transient errors to fail the whole stream because they cannot be recovered from within the application.  Transient errors can be caused by network partitions, Kafka broker failures, `ProducerFencedException`'s from other application instances, and so on.  When the stream encounters transient errors then the current transaction will be aborted before the stream is torn down.  Any produced messages that were not committed will not be available to downstream consumers as long as those consumers are configured with `isolation.level = read_committed`.

For transient errors we can choose to rely on the Kafka producer's configuration to retry, or we can handle it ourselves at the Akka Streams or Application layer.  Using the `RestartSource` (@extref[Akka docs](akka-docs:/stream/stream-error.html#delayed-restarts-with-a-backoff-stage)) we can backoff connection attempts so that we don't hammer the Kafka cluster in a tight loop.

Scala
: @@ snip [transactionalFailureRetry](../../../../tests/src/test/scala/docs//scaladsl/TransactionsExample.scala) { #transactionalFailureRetry }

Java
: @@ snip [transactionalFailureRetry](../../test/java/sample/javadsl/TransactionsExample.java) { #transactionalFailureRetry }

## Caveats

There are several scenarios that this library's implementation of Kafka transactions does not automatically account for.

All of the scenarios covered in the @ref[At-Least-Once Delivery documentation](atleastonce.md) (Multiple Effects per Commit, Non-Sequential Processing, and Conditional Message Processing) are applicable to transactional scenarios as well.

Only one application instance per `transactional.id` is allowed.  If two application instances with the same `transactional.id` are run at the same time then the instance that registers with Kafka's transaction coordinator second will throw a `ProducerFencedException` so it doesn't interfere with transactions in process by the first instance.  To distribute multiple transactional workflows for the same subscription the user must manually subdivide the subscription across multiple instances of the application.  This may be handled internally in future versions.

Any state in the transformation logic is not part of a transaction.  It's left to the user to rebuild state when applying stateful operations with transaction.  It's possible to encode state into messages produced to topics during a transaction.  For example you could produce messages to a topic that represents an event log as part of a transaction.  This event log can be replayed to reconsititue the correct state before the stateful stream resumes consuming again at startup.

Any side effects that occur in the transformation logic is not part of a transaction (i.e. writes to an database).  

## Further Reading

For more information on exactly once and transactions in Kafka please consult the following resources.

* [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) ([Design Document](https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#heading=h.xq0ee1vnpz4o))
* [KIP-129: Streams Exactly-Once Semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics) ([Design Document](https://docs.google.com/document/d/1pGZ8xtOOyGwDYgH5vA6h19zOMMaduFK1DAB8_gBYA2c/edit#heading=h.vkrkjfth3p8p))
* [You Cannot Have Exactly-Once Delivery Redux](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery-redux/) by Tyler Treat
* [Exactly-once Semantics are Possible: Here’s How Kafka Does it](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
