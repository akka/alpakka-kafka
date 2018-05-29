# Consumer

A consumer subscribes to Kafka topics and passes the messages into an Akka Stream.

The underlying implementation is using the `KafkaConsumer`, see @javadoc[Kafka API](org.apache.kafka.clients.consumer.KafkaConsumer) for a description of consumer groups, offsets, and other details.


## Settings

When creating a consumer stream you need to pass in `ConsumerSettings` (@scaladoc[API](akka.kafka.ConsumerSettings)) that define things like:

* serializers for the keys and values
* bootstrap servers of the Kafka cluster
* group id for the consumer, note that offsets are always committed for a given consumer group
* Kafka consumer tuning parameters

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #settings }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #settings }

In addition to programmatic construction of the `ConsumerSettings` (@scaladoc[API](akka.kafka.ConsumerSettings)) it can also be created from configuration (`application.conf`). 

When creating `ConsumerSettings` with the `ActorSystem` (@scaladoc[API](akka.actor.ActorSystem)) settings it uses the config section `akka.kafka.consumer`. The format of these settings files are described in the [Typesafe Config Documentation](https://github.com/lightbend/config#using-hocon-the-json-superset).


@@ snip [flow](../../../../core/src/main/resources/reference.conf) { #consumer-settings }

`ConsumerSettings` (@scaladoc[API](akka.kafka.ConsumerSettings)) can also be created from any other `Config` section with the same layout as above.

See @javadoc[KafkaConsumer API](org.apache.kafka.clients.consumer.KafkaConsumer) and @javadoc[ConsumerConfig API](org.apache.kafka.clients.consumer.ConsumerConfig) for more details regarding settings.


## Offset Storage external to Kafka

The Kafka read offset can either be stored in Kafka (see below), or at a data store of your choice.

`Consumer.plainSource` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer)]) 
and `Consumer.plainPartitionedManualOffsetSource` can be used to emit `ConsumerRecord` (@javadoc[API](org.apache.kafka.clients.consumer.ConsumerRecord)) elements
as received from the underlying `KafkaConsumer`. They do not have support for committing offsets to Kafka. When using
these Sources, either store an offset externally, or use auto-commit (note that auto-commit is by default disabled).

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #settings-autocommit }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #settings-autocommit }

The consumer application doesn't need to use Kafka's built-in offset storage, it can store offsets in a store of its own
choosing. The primary use case for this is allowing the application to store both the offset and the results of the
consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
stronger than the "at-least-once" semantics you get with Kafka's offset commit functionality.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #plainSource }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #plainSource }

For `Consumer.plainSource` the `Subscriptions.assignmentWithOffset` specifies the starting point (offset) for a given consumer group id, topic and partition. The group id is defined in the `ConsumerSettings`.

Alternatively, with `Consumer.plainPartitionedManualOffsetSource` (@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer)]), only the consumer group id and the topic are required on creation.
The starting point is fetched by calling the `getOffsetsOnAssign` function passed in by the user. This function should return
a `Map` of `TopicPartition` (@javadoc[API](org.apache.kafka.common.TopicPartition)) to `Long`, with the `Long` representing the starting point. If a consumer is assigned a partition
that is not included in the `Map` that results from `getOffsetsOnAssign`, the default starting position will be used,
according to the consumer configuration value `auto.offset.reset`. Also note that `Consumer.plainPartitionedManualOffsetSource`
emits tuples of assigned topic-partition and a corresponding source, as in [Source per partition](#source-per-partition).


## Offset Storage in Kafka - committing

The `Consumer.committableSource` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer)])
makes it possible to commit offset positions to Kafka. Compared to auto-commit this gives exact control of when a message is considered consumed.

This is useful when "at-least-once delivery" is desired, as each message will likely be delivered one time but in failure cases could be duplicated.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #atLeastOnce }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #atLeastOnce }

The above example uses separate `mapAsync` stages for processing and committing. This guarantees that for `parallelism` higher than 1 we will keep correct ordering of messages sent for commit. 

Committing the offset for each message as illustrated above is rather slow. It is recommended to batch the commits for better throughput, with the trade-off that more messages may be re-delivered in case of failures.

You can use the Akka Stream `batch` combinator to perform the batching. Note that it will only aggregate elements into batches if the downstream consumer is slower than the upstream producer.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #atLeastOnceBatch }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #atLeastOnceBatch }

If you consume from a not very active topic and it is possible that you don't have any messages received for more than 24 hours, consider enabling periodical commit refresh (`akka.kafka.consumer.commit-refresh-interval` configuration parameters), otherwise offsets might expire in the Kafka storage.

For less active topics timing-based aggregating with `groupedWithin` might be a better choice.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #groupedWithin }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #groupedWithin }


If you commit the offset before processing the message you get "at-most-once" delivery semantics, this is provided by `Consumer.atMostOnceSource`. However, `atMostOnceSource` commits the offset for each message and that is rather slow, batching of commits is recommended.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #atMostOnce }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #atMostOnce }

Maintaining at-least-once delivery semantics requires care, so many risks and solutions are covered in @ref:[At-Least-Once Delivery](atleastonce.md).


## Connecting Producer and Consumer

For cases when you need to read messages from one topic, transform or enrich them, and then write to another topic you can use `Consumer.committableSource` and connect it to a `Producer.commitableSink`. The `commitableSink` will commit the offset back to the consumer when it has successfully published the message.

The `committableSink` accepts messages of type `ProducerMessage.Message` (@scaladoc[API](akka.kafka.ProducerMessage$$Message)) to combine Kafka's `ProducerRecord` with the offset to commit the consumption of the originating message (of type `ConsumerMessage.Committable` (@scaladoc[API](akka.kafka.ConsumerMessage$$Committable))).

Note that there is a risk that something fails after publishing but before committing, so `commitableSink` has "at-least-once" delivery semantics. 

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerSink }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerSink }

Committing messages one-by-one is rather slow, but we can batch the commits to get higher throughput.

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerFlowBatch }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerFlowBatch }

@@@note 

There is a risk that something fails after publishing, but before committing, so `commitableSink` has "at-least-once delivery" semantics.

To get delivery guarantees, please read about @ref[transactions](transactions.md).

@@@


## Source per partition

`Consumer.plainPartitionedSource` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer)])
and `Consumer.committablePartitionedSource` supports tracking the automatic partition assignment from Kafka. When topic-partition is assigned to a consumer this source will emit tuple with assigned topic-partition and a corresponding source. When topic-partition is revoked then corresponding source completes.

Backpressure per partition with batch commit:

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #committablePartitionedSource }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #committablePartitionedSource }

Separate streams per partition:

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #committablePartitionedSource-stream-per-partition }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #committablePartitionedSource-stream-per-partition }


Join flows based on automatically assigned partitions:

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #committablePartitionedSource3 }


## Sharing the KafkaConsumer instance

If you have many streams it can be more efficient to share the underlying `KafkaConsumer` (@javadoc[Kafka API](org.apache.kafka.clients.consumer.KafkaConsumer)) instance. It is shared by creating a `KafkaConsumerActor` (@scaladoc[API](akka.kafka.KafkaConsumerActor)). You need to create the actor and stop it by sending `KafkaConsumerActor.Stop` when it is not needed any longer. You pass the `ActorRef` as a parameter to the `Consumer` 
(@scala[@scaladoc[Consumer API](akka.kafka.scaladsl.Consumer)]@java[@scaladoc[Consumer API](akka.kafka.javadsl.Consumer)])
 factory methods.

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerActor }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerActor }


## Accessing KafkaConsumer metrics

You can access the underlying consumer metrics via the materialized `Control` instance: 

Scala
: @@ snip [consumerMetrics](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerMetrics }

Java
: @@ snip [consumerMetrics](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerMetrics }


## Listening for rebalance events

You may set up an rebalance event listener actor that will be notified when your consumer will be assigned or revoked 
from consuming from specific topic partitions. Two kinds of messages will be sent to this listener actor 

* `akka.kafka.TopicPartitionsAssigned` and 
* `akka.kafka.TopicPartitionsRevoked`, like this:

Scala
: @@ snip [withRebalanceListenerActor](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #withRebalanceListenerActor }

Java
: @@ snip [withRebalanceListenerActor](../../test/java/sample/javadsl/ConsumerExample.java) { #withRebalanceListenerActor }


## Controlled shutdown
The `Source` created with `Consumer.plainSource` and similar  methods materializes to a `Consumer.Control` instance. This can be used to stop the stream in a controlled manner.

When using external offset storage, a call to `Consumer.Control.shutdown()` suffices to complete the `Source`, which starts the completion of the stream.

Scala
: @@ snip [streamShutdown](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #shutdownPlainSource }

Java
: @@ snip [streamShutdown](../../test/java/sample/javadsl/ConsumerExample.java) { #shutdownPlainSource }

When you are using offset storage in Kafka, the shutdown process involves several steps:

1. `Consumer.Control.stop()` to stop producing messages from the `Source`. This does not stop the underlying Kafka Consumer.
2. Wait for the stream to complete, so that a commit request has been made for all offsets of all processed messages (via `commitScaladsl()` or `commitJavadsl()`).
3. `Consumer.Control.shutdown()` to wait for all outstanding commit requests to finish and stop the Kafka Consumer.

To manage this shutdown process, use the `Consumer.DrainingControl` by combining the `Consumer.Control` with the sink's materialized completion future in `mapMaterializedValue'. That control offers the method `drainAndShutdown` which implements the process descibed above. It is recommended to use the same shutdown mechanism also when not using batching to avoid potential race conditions, depending on the exact layout of the stream.

Scala
: @@ snip [streamShutdownBatched](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #shutdownCommitableSource }

Java
: @@ snip [streamShutdownBatched](../../test/java/sample/javadsl/ConsumerExample.java) { #shutdownCommitableSource }
