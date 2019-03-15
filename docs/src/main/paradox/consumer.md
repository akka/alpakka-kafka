# Consumer

A consumer subscribes to Kafka topics and passes the messages into an Akka Stream.

The underlying implementation is using the `KafkaConsumer`, see @javadoc[Kafka API](org.apache.kafka.clients.consumer.KafkaConsumer) for a description of consumer groups, offsets, and other details.


## Choosing a consumer

Alpakka Kafka offers a large variety of consumers that connect to Kafka and stream data. The tables below may help you to find the consumer best suited for your use-case.

### Consumers

These factory methods are part of the @scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer$)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer$)].

| Offsets handling                  | Partition aware | Subscription        | Shared consumer | Factory method | Stream element type |
|-----------------------------------|-----------------|---------------------|-----------------|----------------|---------------------|
| No (auto commit can be enabled)   | No              | Topic or Partition  | No              | `plainSource` | `ConsumerRecord` |
| No (auto commit can be enabled)   | No              | Partition           | Yes             | `plainExternalSource` | `ConsumerRecord` |
| Explicit committing               | No              | Topic or Partition  | No              | `committableSource` | `CommittableMessage` |
| Explicit committing               | No              | Partition           | Yes             | `committableExternalSource` | `CommittableMessage` |
| Explicit committing with metadata | No              | Topic or Partition  | No              | `commitWithMetadataSource` | `CommittableMessage` |
| Offset committed per element      | No              | Topic or Partition  | No              | `atMostOnceSource` | `ConsumerRecord` |
| No (auto commit can be enabled)   | Yes             | Topic or Partition  | No              | `plainPartitionedSource` | `(TopicPartition, Source[ConsumerRecord, ..])` |
| External to Kafka                 | Yes             | Topic or Partition  | No              | `plainPartitionedManualOffsetSource` | `(TopicPartition, Source[ConsumerRecord, ..])` |
| Explicit committing               | Yes             | Topic or Partition  | No              | `committablePartitionedSource` | `(TopicPartition, Source[CommittableMessage, ..])` |
| Explicit committing with metadata | Yes             | Topic or Partition  | No              | `commitWithMetadataPartitionedSource` | `(TopicPartition, Source[CommittableMessage, ..])` |

### Transactional consumers

These factory methods are part of the @scala[@scaladoc[Transactional API](akka.kafka.scaladsl.Transactional$)]@java[@scaladoc[Transactional API](akka.kafka.javadsl.Transactional$)]. For details see @ref[Transactions](transactions.md).

| Offsets handling                  | Partition aware | Shared consumer | Factory method | Stream element type |
|-----------------------------------|-----------------|-----------------|----------------|---------------------|
| Transactional                     | No              | No              | `Transactional.source` | `TransactionalMessage` |


## Settings

When creating a consumer stream you need to pass in `ConsumerSettings` (@scaladoc[API](akka.kafka.ConsumerSettings)) that define things like:

* de-serializers for the keys and values
* bootstrap servers of the Kafka cluster
* group id for the consumer, note that offsets are always committed for a given consumer group
* Kafka consumer tuning parameters

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #settings }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #settings }

In addition to programmatic construction of the `ConsumerSettings` (@scaladoc[API](akka.kafka.ConsumerSettings)) it can also be created from configuration (`application.conf`). 

When creating `ConsumerSettings` with the `ActorSystem` (@scaladoc[API](akka.actor.ActorSystem)) settings it uses the config section `akka.kafka.consumer`. The format of these settings files are described in the [Typesafe Config Documentation](https://github.com/lightbend/config#using-hocon-the-json-superset).


@@ snip [snip](/core/src/main/resources/reference.conf) { #consumer-settings }

`ConsumerSettings` (@scaladoc[API](akka.kafka.ConsumerSettings)) can also be created from any other `Config` section with the same layout as above.

The Kafka documentation [Consumer Configs](http://kafka.apache.org/documentation/#consumerconfigs) lists the settings, their defaults and importance. More detailed explanations are given in the @javadoc[KafkaConsumer API](org.apache.kafka.clients.consumer.KafkaConsumer) and constants are defined in @javadoc[ConsumerConfig API](org.apache.kafka.clients.consumer.ConsumerConfig).


## Offset Storage external to Kafka

The Kafka read offset can either be stored in Kafka (see below), or at a data store of your choice.

`Consumer.plainSource` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer$)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer$)]) 
and `Consumer.plainPartitionedManualOffsetSource` can be used to emit `ConsumerRecord` (@javadoc[Kafka API](org.apache.kafka.clients.consumer.ConsumerRecord)) elements
as received from the underlying `KafkaConsumer`. They do not have support for committing offsets to Kafka. When using
these Sources, either store an offset externally, or use auto-commit (note that auto-commit is disabled by default).

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #settings-autocommit }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #settings-autocommit }

The consumer application doesn't need to use Kafka's built-in offset storage, it can store offsets in a store of its own
choosing. The primary use case for this is allowing the application to store both the offset and the results of the
consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
stronger than the "at-least-once" semantics you get with Kafka's offset commit functionality.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #plainSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #plainSource }

For `Consumer.plainSource` the `Subscriptions.assignmentWithOffset` specifies the starting point (offset) for a given consumer group id, topic and partition. The group id is defined in the `ConsumerSettings`.

Alternatively, with `Consumer.plainPartitionedManualOffsetSource` (@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer$)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer$)]), only the consumer group id and the topic are required on creation.
The starting point is fetched by calling the `getOffsetsOnAssign` function passed in by the user. This function should return
a `Map` of `TopicPartition` (@javadoc[API](org.apache.kafka.common.TopicPartition)) to `Long`, with the `Long` representing the starting point. If a consumer is assigned a partition
that is not included in the `Map` that results from `getOffsetsOnAssign`, the default starting position will be used,
according to the consumer configuration value `auto.offset.reset`. Also note that `Consumer.plainPartitionedManualOffsetSource`
emits tuples of assigned topic-partition and a corresponding source, as in [Source per partition](#source-per-partition).


## Offset Storage in Kafka - committing

The `Consumer.committableSource` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer$)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer$)])
makes it possible to commit offset positions to Kafka. Compared to auto-commit this gives exact control of when a message is considered consumed.

This is useful when "at-least-once" delivery is desired, as each message will likely be delivered one time, but in failure cases could be received more than once.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #atLeastOnce }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #atLeastOnce }

Committing the offset for each message (`withMaxBatch(1)`) as illustrated above is rather slow. It is recommended to batch the commits for better throughput, with the trade-off that more messages may be re-delivered in case of failures.


### Committer sink

You can use a pre-defined `Committer.sink` to perform commits in batches:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #committerSink }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #committerSink }
 
When creating a `Committer.sink` you need to pass in `CommitterSettings` (@scaladoc[API](akka.kafka.CommitterSettings)). These may be created by passing the actor system to read the defaults from the config section `akka.kafka.committer`, or by passing a `Config` (@scaladoc[API](com.typesafe.config.Config)) instance with the same structure.

Table
: | Setting   | Description                                  | Default Value |
|-------------|----------------------------------------------|-----|
| maxBatch    | maximum number of messages to commit at once | 1000 |
| maxInterval | maximum interval between commits             | 10 seconds |
| parallelism | parallelsim for async committing             | 1 |

reference.conf
: @@snip [snip](/core/src/main/resources/reference.conf) { #committer-settings }


The bigger the values are, the less load you put on Kafka and the smaller are chances that committing offsets will become a bottleneck. However, increasing these values also means that in case of a failure you will have to re-process more messages. 

If you consume from a topic with low activity, and possibly no messages arrive for more than 24 hours, consider enabling periodical commit refresh (`akka.kafka.consumer.commit-refresh-interval` configuration parameters), otherwise offsets might expire in the Kafka storage.

### Commit with meta-data

The `Consumer.commitWithMetadataSource` allows you to add metadata to the committed offset based on the last consumed record.

Note that the first offset provided to the consumer during a partition assignment will not contain metadata. This offset can get committed due to a periodic commit refresh (`akka.kafka.consumer.commit-refresh-interval` configuration parmeters) and the commit will not contain metadata.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #commitWithMetadata }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #commitWithMetadata }

If you commit the offset before processing the message you get "at-most-once" delivery semantics, this is provided by `Consumer.atMostOnceSource`. However, `atMostOnceSource` **commits the offset for each message and that is rather slow**, batching of commits is recommended.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #atMostOnce }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #atMostOnce }

Maintaining at-least-once delivery semantics requires care, many risks and solutions are covered in @ref:[At-Least-Once Delivery](atleastonce.md).


## Connecting Producer and Consumer

For cases when you need to read messages from one topic, transform or enrich them, and then write to another topic you can use `Consumer.committableSource` and connect it to a `Producer.committableSink`. The `committableSink` will commit the offset back to the consumer when it has successfully published the message.

The `committableSink` accepts implementations `ProducerMessage.Envelope` (@scaladoc[API](akka.kafka.ProducerMessage$$Envelope)) that contain the offset to commit the consumption of the originating message (of type `ConsumerMessage.Committable` (@scaladoc[API](akka.kafka.ConsumerMessage$$Committable))). See @ref[Producing messages](producer.md#producing-messages) about different implementations of `Envelope` supported.

Note that there is a risk that something fails after publishing but before committing, so `committableSink` has "at-least-once" delivery semantics. 

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #consumerToProducerSink }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerToProducerSink }

As `Producer.committableSink`'s committing of messages one-by-one is rather slow, prefer a flow together with batching of commits with `Committer.sink`.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #consumerToProducerFlow }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerToProducerFlow }

@@@note 

There is a risk that something fails after publishing, but before committing, so `committableSink` has "at-least-once" delivery semantics.

To get delivery guarantees, please read about @ref[transactions](transactions.md).

@@@


## Source per partition

`Consumer.plainPartitionedSource` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer$)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer$)])
, `Consumer.committablePartitionedSource`, and `Consumer.commitWithMetadataPartitionedSource` support tracking the automatic partition assignment from Kafka. When a topic-partition is assigned to a consumer, this source will emit a tuple with the assigned topic-partition and a corresponding source. When a topic-partition is revoked, the corresponding source completes.


Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #committablePartitionedSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #committablePartitionedSource }

Separate streams per partition:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #committablePartitionedSource-stream-per-partition }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #committablePartitionedSource-stream-per-partition }


Join flows based on automatically assigned partitions:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #committablePartitionedSource3 }


## Sharing the KafkaConsumer instance

If you have many streams it can be more efficient to share the underlying `KafkaConsumer` (@javadoc[Kafka API](org.apache.kafka.clients.consumer.KafkaConsumer)) instance. It is shared by creating a `KafkaConsumerActor` (@scaladoc[API](akka.kafka.KafkaConsumerActor$)). You need to create the actor and stop it by sending `KafkaConsumerActor.Stop` when it is not needed any longer. You pass the `ActorRef` as a parameter to the `Consumer` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer$)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer$)])
 factory methods.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/PartitionExamples.scala) { #consumerActor }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerActor }


## Accessing KafkaConsumer metrics

You can access the underlying consumer metrics via the materialized `Control` instance: 

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/PartitionExamples.scala) { #consumerMetrics }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerMetrics }


## Accessing KafkaConsumer metadata

Accessing of Kafka consumer metadata is possible as described in @ref[Consumer Metadata](consumer-metadata.md).


## Listening for rebalance events

You may set up an rebalance event listener actor that will be notified when your consumer will be assigned or revoked 
from consuming from specific topic partitions. Two kinds of messages will be sent to this listener actor 

* `akka.kafka.TopicPartitionsAssigned` and 
* `akka.kafka.TopicPartitionsRevoked`, like this:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #withRebalanceListenerActor }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #withRebalanceListenerActor }


## Controlled shutdown
The `Source` created with `Consumer.plainSource` and similar  methods materializes to a `Consumer.Control` (@scala[@scaladoc[API](akka.kafka.scaladsl.Consumer$$Control)]@java[@scaladoc[API](akka.kafka.javadsl.Consumer$$Control)]) 
instance. This can be used to stop the stream in a controlled manner.

When using external offset storage, a call to `Consumer.Control.shutdown()` suffices to complete the `Source`, which starts the completion of the stream.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #shutdownPlainSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #shutdownPlainSource }

When you are using offset storage in Kafka, the shutdown process involves several steps:

1. `Consumer.Control.stop()` to stop producing messages from the `Source`. This does not stop the underlying Kafka Consumer.
2. Wait for the stream to complete, so that a commit request has been made for all offsets of all processed messages (via `Committer.sink/flow`, `commitScaladsl()` or `commitJavadsl()`).
3. `Consumer.Control.shutdown()` to wait for all outstanding commit requests to finish and stop the Kafka Consumer.

### Draining control

To manage this shutdown process, use the `Consumer.DrainingControl` 
(@scala[@scaladoc[API](akka.kafka.scaladsl.Consumer$$DrainingControl)]@java[@scaladoc[API](akka.kafka.javadsl.Consumer$$DrainingControl)])
by combining the `Consumer.Control` with the sink's materialized completion future in `mapMaterializedValue`. That control offers the method `drainAndShutdown` which implements the process descibed above.

Note: The `ConsummerSettings` `stop-timeout` delays stopping the Kafka Consumer and the stream, but when using `drainAndShutdown` that delay is not required and can be set to zero (as below).

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #shutdownCommittableSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #shutdownCommittableSource }


@@@ index

* [subscription](subscription.md)
* [metadata](consumer-metadata.md)

@@@
