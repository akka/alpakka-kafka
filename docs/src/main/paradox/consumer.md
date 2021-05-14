---
project.description: Consume messages from Apache Kafka in Akka Streams sources and their commit offsets to Kafka.
---
# Consumer

A consumer subscribes to Kafka topics and passes the messages into an Akka Stream.

The underlying implementation is using the `KafkaConsumer`, see @javadoc[Kafka API](org.apache.kafka.clients.consumer.KafkaConsumer) for a description of consumer groups, offsets, and other details.


## Choosing a consumer

Alpakka Kafka offers a large variety of consumers that connect to Kafka and stream data. The tables below may help you to find the consumer best suited for your use-case.

### Consumers

These factory methods are part of the @apidoc[Consumer$] API.

| Offsets handling                        | Partition aware | Subscription        | Shared consumer | Factory method | Stream element type |
|-----------------------------------------|-----------------|---------------------|-----------------|----------------|---------------------|
| No (auto commit can be enabled)         | No              | Topic or Partition  | No              | `plainSource` | `ConsumerRecord` |
| No (auto commit can be enabled)         | No              | Partition           | Yes             | `plainExternalSource` | `ConsumerRecord` |
| Explicit committing                     | No              | Topic or Partition  | No              | `committableSource` | `CommittableMessage` |
| Explicit committing                     | No              | Partition           | Yes             | `committableExternalSource` | `CommittableMessage` |
| Explicit committing with metadata       | No              | Topic or Partition  | No              | `commitWithMetadataSource` | `CommittableMessage` |
| Explicit committing (with metadata)     | No              | Topic or Partition  | No              | `sourceWithOffsetContext` | `ConsumerRecord` |
| Offset committed per element            | No              | Topic or Partition  | No              | `atMostOnceSource` | `ConsumerRecord` |
| No (auto commit can be enabled)         | Yes             | Topic or Partition  | No              | `plainPartitionedSource` | `(TopicPartition, Source[ConsumerRecord, ..])` |
| External to Kafka                       | Yes             | Topic or Partition  | No              | `plainPartitionedManualOffsetSource` | `(TopicPartition, Source[ConsumerRecord, ..])` |
| Explicit committing                     | Yes             | Topic or Partition  | No              | `committablePartitionedSource` | `(TopicPartition, Source[CommittableMessage, ..])`     |
| External to Kafka & Explicit Committing | Yes             | Topic or Partition  | No              | `committablePartitionedManualOffsetSource` | `(TopicPartition, Source[CommittableMessage, ..])` |
| Explicit committing with metadata       | Yes             | Topic or Partition  | No              | `commitWithMetadataPartitionedSource` | `(TopicPartition, Source[CommittableMessage, ..])`  |

### Transactional consumers

These factory methods are part of the @apidoc[Transactional$]. For details see @ref[Transactions](transactions.md).

| Offsets handling                  | Partition aware | Shared consumer | Factory method | Stream element type |
|-----------------------------------|-----------------|-----------------|----------------|---------------------|
| Transactional                     | No              | No              | `Transactional.source` | `TransactionalMessage` |
| Transactional                     | No              | No              | `Transactional.sourceWithOffsetContext` | `ConsumerRecord` |


## Settings

When creating a consumer source you need to pass in @apidoc[ConsumerSettings] that define things like:

* de-serializers for the keys and values
* bootstrap servers of the Kafka cluster (see @ref:[Service discovery](discovery.md) to defer the server configuration)
* group id for the consumer, note that offsets are always committed for a given consumer group
* Kafka consumer tuning parameters

Alpakka Kafka's defaults for all settings are defined in `reference.conf` which is included in the library JAR.

Important consumer settings
: | Setting   | Description                                  |
|-------------|----------------------------------------------|
| stop-timeout | The stage will delay stopping the internal actor to allow processing of messages already in the stream (required for successful committing). This can be set to 0 for streams using @apidoc[Consumer.DrainingControl] |
| kafka-clients | Section for properties passed unchanged to the Kafka client (see @extref:[Kafka's Consumer Configs](kafka:/documentation.html#consumerconfigs)) |
| connection-checker | Configuration to let the stream fail if the connection to the Kafka broker fails. |

reference.conf (HOCON)
: @@ snip [snip](/core/src/main/resources/reference.conf) { #consumer-settings }

The Kafka documentation @extref:[Consumer Configs](kafka:/documentation.html#consumerconfigs) lists the settings, their defaults and importance. More detailed explanations are given in the @javadoc[KafkaConsumer](org.apache.kafka.clients.consumer.KafkaConsumer) API and constants are defined in @javadoc[ConsumerConfig](org.apache.kafka.clients.consumer.ConsumerConfig) API.


### Programmatic construction

Stream-specific settings like the de-serializers and consumer group ID should be set programmatically. Settings that apply to many consumers may be set in `application.conf` or use @ref:[config inheritance](#config-inheritance).

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #settings }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #settings }


### Config inheritance

@apidoc[ConsumerSettings$] are created from configuration in `application.conf` (with defaults in `reference.conf`). The format of these settings files are described in the [HOCON Config Documentation](https://github.com/lightbend/config#using-hocon-the-json-superset). A recommended setup is to rely on config inheritance as below:

application.conf (HOCON)
: @@ snip [app.conf](/tests/src/test/resources/application.conf) { #consumer-config-inheritance }

Read the settings that inherit the defaults from "akka.kafka.consumer" settings:

Scala
: @@ snip [read](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #config-inheritance } 

Java
: @@ snip [read](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #config-inheritance } 


## Offset Storage external to Kafka

The Kafka read offset can either be stored in Kafka (see below), or at a data store of your choice.

@apidoc[Consumer.plainSource](Consumer$) { java="#plainSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.javadsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.kafka.javadsl.Consumer.Control]" scala="#plainSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.scaladsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.kafka.scaladsl.Consumer.Control]" } 
and 
@apidoc[Consumer.plainPartitionedManualOffsetSource](Consumer$) { java="#plainPartitionedManualOffsetSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.AutoSubscription,getOffsetsOnAssign:java.util.function.Function[java.util.Set[org.apache.kafka.common.TopicPartition],java.util.concurrent.CompletionStage[java.util.Map[org.apache.kafka.common.TopicPartition,Long]]]):akka.stream.javadsl.Source[akka.japi.Pair[org.apache.kafka.common.TopicPartition,akka.stream.javadsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.NotUsed]],akka.kafka.javadsl.Consumer.Control]" scala="#plainPartitionedManualOffsetSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.AutoSubscription,getOffsetsOnAssign:Set[org.apache.kafka.common.TopicPartition]=%3Escala.concurrent.Future[Map[org.apache.kafka.common.TopicPartition,Long]],onRevoke:Set[org.apache.kafka.common.TopicPartition]=%3EUnit):akka.stream.scaladsl.Source[(org.apache.kafka.common.TopicPartition,akka.stream.scaladsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.NotUsed]),akka.kafka.scaladsl.Consumer.Control]" }
can be used to emit @javadoc[ConsumerRecord](org.apache.kafka.clients.consumer.ConsumerRecord) elements
as received from the underlying @javadoc[KafkaConsumer](org.apache.kafka.clients.consumer.KafkaConsumer). They do not have support for committing offsets to Kafka. When using
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

For 
@apidoc[Consumer.plainSource](Consumer$) { java="#plainSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.javadsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.kafka.javadsl.Consumer.Control]" scala="#plainSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.scaladsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.kafka.scaladsl.Consumer.Control]" } 
the @apidoc[Subscriptions.assignmentWithOffset](Subscriptions$) specifies the starting point (offset) for a given consumer group id, topic and partition. The group id is defined in the @apidoc[ConsumerSettings$].

Alternatively, with @apidoc[Consumer.plainPartitionedManualOffsetSource](Consumer$), only the consumer group id and the topic are required on creation.
The starting point is fetched by calling the `getOffsetsOnAssign` function passed in by the user. This function should return
a `Map` of @javadoc[TopicPartition](org.apache.kafka.common.TopicPartition) to `Long`, with the `Long` representing the starting point. If a consumer is assigned a partition
that is not included in the `Map` that results from `getOffsetsOnAssign`, the default starting position will be used,
according to the consumer configuration value `auto.offset.reset`. Also note that @apidoc[Consumer.plainPartitionedManualOffsetSource](Consumer$)
emits tuples of assigned topic-partition and a corresponding source, as in [Source per partition](#source-per-partition).


## Offset Storage in Kafka - committing

The 
@apidoc[Consumer.committableSource](Consumer$) { java="#committableSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.javadsl.Source[akka.kafka.ConsumerMessage.CommittableMessage[K,V],akka.kafka.javadsl.Consumer.Control]" scala="#committableSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.scaladsl.Source[akka.kafka.ConsumerMessage.CommittableMessage[K,V],akka.kafka.scaladsl.Consumer.Control]" } 
makes it possible to commit offset positions to Kafka. Compared to auto-commit this gives exact control of when a message is considered consumed.

This is useful when "at-least-once" delivery is desired, as each message will likely be delivered one time, but in failure cases could be received more than once.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #atLeastOnce }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #atLeastOnce }

Committing the offset for each message (`withMaxBatch(1)`) as illustrated above is rather slow. It is recommended to batch the commits for better throughput, in cases when upstream fails the `Committer` will try to commit the offsets collected before the error.


### Committer sink

You can use a pre-defined @apidoc[Committer.sink](Committer$) to perform commits in batches:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #committerSink }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #committerSink }
 
When creating a @apidoc[Committer.sink](Committer$) you need to pass in @apidoc[CommitterSettings$]. These may be created by passing the actor system to read the defaults from the config section `akka.kafka.committer`, or by passing a @scaladoc[Config](com.typesafe.config.Config) instance with the same structure.

Table
: | Setting   | Description                                  | Default Value |
|-------------|----------------------------------------------|---------------|
| maxBatch    | maximum number of messages to commit at once | 1000 |
| maxInterval | maximum interval between commits             | 10 seconds |
| parallelism | maximum number of commit batches in flight   | 100 |

reference.conf
: @@snip [snip](/core/src/main/resources/reference.conf) { #committer-settings }

All commit batches are aggregated internally and passed on to Kafka very often (in every poll cycle), the Committer settings configure how the stream sends the offsets to the internal actor which communicates with the Kafka broker. Increasing these values means that in case of a failure you may have to re-process more messages.

If you use Kafka older than version 2.1.0 and consume from a topic with low activity, and possibly no messages arrive for more than 24 hours, consider enabling periodical commit refresh (`akka.kafka.consumer.commit-refresh-interval` configuration parameters), otherwise offsets might expire in the Kafka storage. This has been fixed in Kafka 2.1.0 (See [KAFKA-4682](https://issues.apache.org/jira/browse/KAFKA-4682)).

#### Committer variants

These factory methods are part of the @apidoc[Committer$].

| Factory method          | Stream element type                  | Emits |
|-------------------------|--------------------------------------|--------------|
| `sink`                  | `Committable`                        | N/A       |
| `sinkWithOffsetContext` | Any (`CommittableOffset` in context) | N/A       |
| `flow`                  | `Committable`                        | `Done`    |
| `batchFlow`             | `Committable`                        | `CommittableOffsetBatch`  |
| `flowWithOffsetContext` | Any (`CommittableOffset` in context) | `NotUsed` (`CommittableOffsetBatch` in context) |


### Commit with meta-data

The @apidoc[Consumer.commitWithMetadataSource](Consumer$) allows you to add metadata to the committed offset based on the last consumed record.

Note that the first offset provided to the consumer during a partition assignment will not contain metadata. This offset can get committed due to a periodic commit refresh (`akka.kafka.consumer.commit-refresh-interval` configuration parameters) and the commit will not contain metadata.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #commitWithMetadata }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #commitWithMetadata }

## Offset Storage in Kafka & external

In some cases you may wish to use external offset storage as your primary means to manage offsets, but also commit offsets to Kafka. 
This gives you all the benefits of controlling offsets described in @ref:[Offset Storage external to Kafka](#offset-storage-external-to-kafka) and allows you to use tooling in the Kafka ecosystem to track consumer group lag. 
You can use the @apidoc[Consumer.committablePartitionedManualOffsetSource](Consumer$) source, which emits a @apidoc[ConsumerMessage.CommittableMessage], to seek to appropriate offsets on startup, do your processing, commit to external storage, and then commit offsets back to Kafka. 
This will only provide at-least-once guarantees for your consumer group lag monitoring because it's possible for a failure between storing your offsets externally and committing to Kafka, but it will give you a more accurate representation of consumer group lag then when turning on auto commits with the `enable.auto.commit` consumer property.

## Consume "at-most-once"

If you commit the offset before processing the message you get "at-most-once" delivery semantics, this is provided by @apidoc[Consumer.atMostOnceSource](Consumer$). However, `atMostOnceSource` **commits the offset for each message and that is rather slow**, batching of commits is recommended. If your "at-most-once" requirements are more relaxed, consider a @apidoc[Consumer.plainSource](Consumer$) and enable Kafka's auto committing with `enable.auto.commit = true`.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #atMostOnce }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #atMostOnce }


## Consume "at-least-once"

How to achieve at-least-once delivery semantics is covered in @ref:[At-Least-Once Delivery](atleastonce.md).


## Connecting Producer and Consumer

For cases when you need to read messages from one topic, transform or enrich them, and then write to another topic you can use @apidoc[Consumer.committableSource](Consumer$) and connect it to a @apidoc[Producer.committableSink](Producer$). The `committableSink` will commit the offset back to the consumer regularly.

The `committableSink` accepts implementations @apidoc[ProducerMessage.Envelope] that contain the offset to commit the consumption of the originating message (of type @apidoc[akka.kafka.ConsumerMessage.Committable]). See @ref[Producing messages](producer.md#producing-messages) about different implementations of @apidoc[ProducerMessage.Envelope].

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #consumerToProducerSink }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerToProducerSink }

@@@note 

There is a risk that something fails after publishing, but before committing, so `committableSink` has "at-least-once" delivery semantics.

To get delivery guarantees, please read about @ref[transactions](transactions.md).

@@@

## Source per partition

@apidoc[Consumer.plainPartitionedSource](Consumer$)
, @apidoc[Consumer.committablePartitionedSource](Consumer$), and @apidoc[Consumer.commitWithMetadataPartitionedSource](Consumer$) support tracking the automatic partition assignment from Kafka. When a topic-partition is assigned to a consumer, this source will emit a tuple with the assigned topic-partition and a corresponding source. When a topic-partition is revoked, the corresponding source completes.


Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #committablePartitionedSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #committablePartitionedSource }

Separate streams per partition:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #committablePartitionedSource-stream-per-partition }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #committablePartitionedSource-stream-per-partition }


## Sharing the KafkaConsumer instance

If you have many streams it can be more efficient to share the underlying @javadoc[KafkaConsumer](org.apache.kafka.clients.consumer.KafkaConsumer) instance. 
It is shared by creating a @apidoc[akka.kafka.KafkaConsumerActor$]. 
You need to create the actor and stop it by sending `KafkaConsumerActor.Stop` when it is not needed any longer. 
You pass the classic @apidoc[akka.actor.ActorRef] as a parameter to the @apidoc[Consumer](Consumer$) factory methods.

When using a typed @apidoc[akka.actor.typed.ActorSystem] you can create the @apidoc[akka.kafka.KafkaConsumerActor$] by using the Akka typed adapter to create a classic @apidoc[akka.actor.ActorRef].
Then you can carry on using the existing Alpakka Kafka API.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/PartitionExamples.scala) { #consumerActorTyped }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerActorTyped }

Using the @apidoc[akka.kafka.KafkaConsumerActor$].

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


## Controlled shutdown
The @apidoc[Source] created with @apidoc[Consumer.plainSource](Consumer$) and similar methods materializes to a @apidoc[akka.kafka.(javadsl|scaladsl).Consumer.Control] instance. This can be used to stop the stream in a controlled manner.

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

To manage this shutdown process, use the @apidoc[Consumer.DrainingControl]
by combining the @apidoc[Consumer.Control] with the sink's materialized completion future in `toMat` or in `mapMaterializedValue` with @scala[`DrainingControl.apply`]@java[`Consumer::createDrainingControl`]. That control offers the method `drainAndShutdown` which implements the process described above. The wrapped stream completion signal is available through the @scala[`streamCompletion`]@java[`streamCompletion()`] accessor.

Note: The @apidoc[ConsumerSettings] `stop-timeout` delays stopping the Kafka Consumer and the stream, but when using `drainAndShutdown` that delay is not required and can be set to zero (as below).

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #shutdownCommittableSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #shutdownCommittableSource }


@@@ index

* [subscription](subscription.md)
* [rebalance](consumer-rebalance.md)
* [metadata](consumer-metadata.md)

@@@
