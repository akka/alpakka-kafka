---
project.description: Produce messages to Apache Kafka topics from Akka Streams with Alpakka Kafka.
---
# Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

The underlying implementation is using the `KafkaProducer`, see the @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) API for details.

## Choosing a producer

Alpakka Kafka offers producer flows and sinks that connect to Kafka and write data. The tables below may help you to find the producer best suited for your use-case.

For use-cases that don't benefit form Akka Streams, @ref[Element Producer](element-producer.md) offers a simple `send` API.

### Producers

These factory methods are part of the @apidoc[Producer$] API.

| Factory method    | May use shared producer | Stream element type | Pass-through | Context |
|-------------------|-------------------------|---------------------|--------------|---------|
| `plainSink`       | Yes                     | `ProducerRecord`    | N/A          | N/A     |
| `flexiFlow`       | Yes                     | `Envelope`          | Any          | N/A     |
| `flowWithContext` | Yes                     | `Envelope`          | No           | Any     |

### Committing producer sinks

These producers produce messages to Kafka and commit the offsets of incoming messages regularly.

| Factory method                     | May use shared producer | Stream element type | Pass-through  | Context       |
|------------------------------------|-------------------------|---------------------|---------------|---------------|
| `committableSink`                  | Yes                     | `Envelope`          | `Committable` | N/A           |
| `committableSinkWithOffsetContext` | Yes                     | `Envelope`          | Any           | `Committable` |

For details about the batched committing see @ref:[Consumer: Offset Storage in Kafka - committing](consumer.md#offset-storage-in-kafka-committing).

### Transactional producers

These factory methods are part of the @apidoc[Transactional$] API. For details see @ref[Transactions](transactions.md).
Alpakka Kafka must manage the producer when using transactions.

| Factory method          | May use shared producer | Stream element type | Pass-through |
|-------------------------|-------------------------|---------------------|--------------|
| `sink`                  | No                      | `Envelope`          | N/A          |
| `flow`                  | No                      | `Envelope`          | No           |
| `sinkWithOffsetContext` | No                      | `Envelope`          | N/A          |
| `flowWithOffsetContext` | No                      | `Envelope`          | No           |


## Settings

When creating a producer stream you need to pass in @apidoc[ProducerSettings$] that define things like:

* bootstrap servers of the Kafka cluster (see @ref:[Service discovery](discovery.md) to defer the server configuration)
* serializers for the keys and values
* tuning parameters

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #settings }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #settings }

In addition to programmatic construction of the @apidoc[ProducerSettings$] it can also be created from configuration (`application.conf`). 

When creating @apidoc[ProducerSettings$] with the @apidoc[akka.actor.ActorSystem] settings it uses the config section `akka.kafka.producer`. The format of these settings files are described in the [Typesafe Config Documentation](https://github.com/lightbend/config#using-hocon-the-json-superset).

@@ snip [snip](/core/src/main/resources/reference.conf) { #producer-settings }

@apidoc[ProducerSettings$] can also be created from any other `Config` section with the same layout as above.

See Kafka's @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) and @javadoc[ProducerConfig](org.apache.kafka.clients.producer.ProducerConfig) for more details regarding settings.


## Producer as a Sink

@apidoc[Producer.plainSink](Producer$) { java="#plainSink[K,V](settings:akka.kafka.ProducerSettings[K,V]):akka.stream.javadsl.Sink[org.apache.kafka.clients.producer.ProducerRecord[K,V],java.util.concurrent.CompletionStage[akka.Done]]" scala="#plainSink[K,V](settings:akka.kafka.ProducerSettings[K,V]):akka.stream.scaladsl.Sink[org.apache.kafka.clients.producer.ProducerRecord[K,V],scala.concurrent.Future[akka.Done]]" } 
is the easiest way to publish messages. The sink consumes the Kafka type @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord) which contains 

1. a topic name to which the record is being sent, 
1. an optional partition number, 
1. an optional key, and
1. a value.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #plainSink }
  The materialized value of the sink is a `Future[Done]` which is completed with `Done` when the stream completes, or with with an exception in case an error occurs.

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #plainSink }
  The materialized value of the sink is a `CompletionStage<Done>` which is completed with `Done` when the stream completes, or with an exception in case an error occurs.


## Producing messages

Sinks and flows accept implementations of @apidoc[ProducerMessage.Envelope] as input. They contain an extra field to pass through data, the so called `passThrough`. Its value is passed through the flow and becomes available in the @apidoc[akka.kafka.ProducerMessage.Results]' `passThrough()`. It can for example hold a @apidoc[akka.kafka.ConsumerMessage.CommittableOffset] or @apidoc[ConsumerMessage.CommittableOffsetBatch] from a @apidoc[Consumer.committableSource](Consumer$) that can be committed after publishing to Kafka. 


### Produce a single message to Kafka

To create one message to a Kafka topic, use the @apidoc[ProducerMessage.Message] type as in

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #singleMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #singleMessage }


For flows the @apidoc[ProducerMessage.Message]s continue as @apidoc[akka.kafka.ProducerMessage.Result] elements containing: 
 
 1. the original input message,
 1. the record metadata (Kafka @javadoc[RecordMetadata](org.apache.kafka.clients.producer.RecordMetadata) API), and
 1. access to the `passThrough` within the message.  


### Let one stream element produce multiple messages to Kafka

The @apidoc[ProducerMessage.MultiMessage] contains a list of @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord)s to produce multiple messages to Kafka topics.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #multiMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #multiMessage }

For flows the @apidoc[ProducerMessage.MultiMessage]s continue as @apidoc[akka.kafka.ProducerMessage.MultiResult] elements containing: 
 
 1. a list of @apidoc[ProducerMessage.MultiResultPart] with
    1. the original input message,
    1. the record metadata (Kafka @javadoc[RecordMetadata](org.apache.kafka.clients.producer.RecordMetadata) API), and
 1. the `passThrough` data.  



### Let a stream element pass through, without producing a message to Kafka

The @apidoc[ProducerMessage.PassThroughMessage] allows to let an element pass through a Kafka flow without producing a new message to a Kafka topic. This is primarily useful with Kafka commit offsets and transactions, so that these can be committed without producing new messages.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #passThroughMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #passThroughMessage }


For flows the @apidoc[ProducerMessage.PassThroughMessage]s continue as @apidoc[ProducerMessage.PassThroughResult] elements containing the `passThrough` data.  


## Producer as a Flow

@apidoc[Producer.flexiFlow](Producer$) { java="#flexiFlow[K,V,PassThrough](settings:akka.kafka.ProducerSettings[K,V]):akka.stream.javadsl.Flow[akka.kafka.ProducerMessage.Envelope[K,V,PassThrough],akka.kafka.ProducerMessage.Results[K,V,PassThrough],akka.NotUsed]" scala="#flexiFlow[K,V,PassThrough](settings:akka.kafka.ProducerSettings[K,V]):akka.stream.scaladsl.Flow[akka.kafka.ProducerMessage.Envelope[K,V,PassThrough],akka.kafka.ProducerMessage.Results[K,V,PassThrough],akka.NotUsed]" }
allows the stream to continue after publishing messages to Kafka. It accepts implementations of @apidoc[ProducerMessage.Envelope] as input, which continue in the flow as implementations of @apidoc[ProducerMessage.Results]. 
 

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #flow }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #flow }


## Connecting a Producer to a Consumer

The `passThrough` can for example hold a @apidoc[akka.kafka.ConsumerMessage.Committable] that can be committed after publishing to Kafka. 

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #consumerToProducerSink }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerToProducerSink }


## Sharing the KafkaProducer instance

The underlying @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) is thread safe and sharing a single producer instance across streams will generally be faster than having multiple instances.
You cannot share `KafkaProducer` with the Transactional flows and sinks.

To create a @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) from the Kafka connector settings described [above](#settings), the @apidoc[ProducerSettings] contains the factory methods @scala[`createKafkaProducerAsync`]@java[`createKafkaProducerCompletionStage`] and `createKafkaProducer` (blocking for asynchronous enriching).

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #producer }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #producer }

The @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) instance (or @scala[Future]@java[CompletionStage]) is passed as a parameter to @apidoc[ProducerSettings] using the methods `withProducer` and `withProducerFactory`.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #plainSinkWithProducer }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #plainSinkWithProducer }


## Accessing KafkaProducer metrics

By passing an explicit reference to a @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) (as shown in the previous section) its metrics become accessible. Refer to the Kafka @javadoc[MetricName](org.apache.kafka.common.MetricName) API for more details.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #producerMetrics }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #producerMetrics }
