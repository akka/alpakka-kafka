# Consumer

A consumer is used for subscribing to Kafka topics.

The underlying implementation is using the `KafkaConsumer`, see [Javadoc](http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) for description of consumer groups, offsets, and other details.

## Example Code

For the examples in this section we use the following two dummy classes to illustrate how messages can be consumed.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #db }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #db }


Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #rocket }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #rocket }

## Settings

When creating a consumer stream you need to pass in `ConsumerSettings` that define things like:

* bootstrap servers of the Kafka cluster
* group id for the consumer, note that offsets are always committed for a given consumer group
* serializers for the keys and values
* tuning parameters

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #settings }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #settings }

In addition to programmatic construction of the `ConsumerSettings` it can also be created from configuration (`application.conf`). By default when creating `ConsumerSettings` with the `ActorSystem` parameter it uses the config section `akka.kafka.consumer`.

@@ snip [flow](../../../../core/src/main/resources/reference.conf) { #consumer-settings }

`ConsumerSettings` can also be created from any other `Config` section with the same layout as above.

See [KafkaConsumer Javadoc](http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) and [ConsumerConfig Javadoc](http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/ConsumerConfig.html) for details.

## External Offset Storage

The `Consumer.plainSource` emits `ConsumerRecord` elements (as received from the underlying `KafkaConsumer`).
It does not have support for committing offsets to Kafka. When using this Source, either store an offset externally or use auto-commit (note that auto-commit is by default disabled).

The consumer application doesn't need to use Kafka's built-in offset storage, it can store offsets in a store of its own
choosing. The primary use case for this is allowing the application to store both the offset and the results of the
consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
stronger than the "at-least once" semantics you get with Kafka's offset commit functionality.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #plainSource }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #plainSource }

Note how the starting point (offset) is assigned for a given consumer group id,
topic and partition. The group id is defined in the `ConsumerSettings`.

## Offset Storage in Kafka

The `Consumer.committableSource` makes it possible to commit offset positions to Kafka.

Compared to auto-commit this gives exact control of when a message is considered consumed.

If you need to store offsets in anything other than Kafka, `plainSource` should be used instead of this API.

This is useful when "at-least once delivery" is desired, as each message will likely be delivered one time but in failure cases could be duplicated.

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

`groupedWithin` is an alternative way of aggregating elements:

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #groupedWithin }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #groupedWithin }


If you commit the offset before processing the message you get "at-most once delivery" semantics, and for that there is a `Consumer.atMostOnceSource`. However, `atMostOnceSource` commits the offset for each message and that is rather slow, batching of commits is recommended.

Scala
: @@ snip [dummy](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #atMostOnce }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/ConsumerExample.java) { #atMostOnce }


## Connecting Producer and Consumer

For cases when you need to read messages from one topic, transform or enrich them, and then write to another topic you can use `Consumer.committableSource` and connect it to a `Producer.commitableSink`. The `commitableSink` will commit the offset back to the consumer when it has successfully published the message.

Note that there is a risk that something fails after publishing but before committing, so `commitableSink` has "at-least once delivery" semantics.

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerSink }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerSink }

As mentioned earlier, committing each message is rather slow and we can batch the commits to get higher throughput.

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerFlowBatch }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerFlowBatch }

## Source per partition

`Consumer.plainPartitionedSource` and `Consumer.committablePartitionedSource` supports tracking the automatic partition assignment from Kafka. When topic-partition is assigned to a consumer this source will emit tuple with assigned topic-partition and a corresponding source. When topic-partition is revoked then corresponding source completes.

Backpressure per partition with batch commit:

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #committablePartitionedSource }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #committablePartitionedSource }

Separate streams per partition:

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #committablePartitionedSource2 }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #committablePartitionedSource2 }


Join flows based on automatically assigned partitions:

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #committablePartitionedSource3 }

## Sharing KafkaConsumer

If you have many streams it can be more efficient to share the underlying `KafkaConsumer`. That can be shared via the `KafkaConsumerActor`. You need to create the actor and stop it when it is not needed any longer. You pass the `ActorRef` as a parameter to the `Consumer` factory methods.

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerActor }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerActor }

## Restarting Consumer

Shutdown and restart the stream.

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #restartConsumer }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #restartConsumer }



