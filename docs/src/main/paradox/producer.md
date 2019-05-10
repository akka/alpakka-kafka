# Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

The underlying implementation is using the `KafkaProducer`, see the @javadoc[Kafka API](org.apache.kafka.clients.producer.KafkaProducer) for details.

## Choosing a producer

Alpakka Kafka offers producer flows and sinks that connect to Kafka and write data. The tables below may help you to find the producer best suited for your use-case.

### Producers

These factory methods are part of the @scala[@scaladoc[Producer API](akka.kafka.scaladsl.Producer$)]@java[@scaladoc[Producer API](akka.kafka.javadsl.Producer$)].

| Shared producer | Factory method    | Stream element type | Pass-through |
|-----------------|-------------------|---------------------|--------------|
| Available       | `plainSink`       | `ProducerRecord`    | -   |
| Available       | `flexiFlow`       | `Envelope`          | Any |
| Available       | `flowWithContext` | `Envelope`          | No  |


### Transactional producers

These factory methods are part of the @scala[@scaladoc[Transactional API](akka.kafka.scaladsl.Transactional$)]@java[@scaladoc[Transactional API](akka.kafka.javadsl.Transactional$)]. For details see @ref[Transactions](transactions.md).

| Shared producer | Factory method    | Stream element type | Pass-through |
|-----------------|-------------------|---------------------|--------------|
| No              | `sink`            | `Envelope`          | -  |
| No              | `sinkWithContext` | `Envelope`          | -  |
| No              | `flow`            | `Envelope`          | No |
| No              | `flowWithContext` | `Envelope`          | No |


## Settings

When creating a producer stream you need to pass in `ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) that define things like:

* bootstrap servers of the Kafka cluster
* serializers for the keys and values
* tuning parameters

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #settings }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #settings }

In addition to programmatic construction of the `ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) it can also be created from configuration (`application.conf`). 

When creating `ProducerSettings` with the `ActorSystem` (@scaladoc[API](akka.actor.ActorSystem)) settings it uses the config section `akka.kafka.producer`. The format of these settings files are described in the [Typesafe Config Documentation](https://github.com/lightbend/config#using-hocon-the-json-superset).

@@ snip [snip](/core/src/main/resources/reference.conf) { #producer-settings }

`ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) can also be created from any other `Config` section with the same layout as above.

See @javadoc[KafkaProducer API](org.apache.kafka.clients.producer.KafkaProducer) and @javadoc[ProducerConfig API](org.apache.kafka.clients.producer.ProducerConfig) for more details regarding settings.


## Producer as a Sink

`Producer.plainSink` 
(@scala[@scaladoc[Producer API](akka.kafka.scaladsl.Producer$)]@java[@scaladoc[Producer API](akka.kafka.javadsl.Producer$)]) 
is the easiest way to publish messages. The sink consumes the Kafka type `ProducerRecord` (@javadoc[Kafka API](org.apache.kafka.clients.producer.ProducerRecord)) which contains 

1. a topic name to which the record is being sent, 
1. an optional partition number, 
1. an optional key, and
1. a value.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #plainSink }
  The materialized value of the sink is a `Future[Done]` which is completed with `Done` when the stream completes, or with with an exception in case an error occurs.

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #plainSink }
  The materialized value of the sink is a `CompletionStage<Done>` which is completed with `Done` when the stream completes, or with an exception in case an error occurs.


## Producing messages

Sinks and flows accept implementations of `ProducerMessage.Envelope` (@scaladoc[API](akka.kafka.ProducerMessage$$Envelope)) as input. They contain an extra field to pass through data, the so called `passThrough`. Its value is passed through the flow and becomes available in the `ProducerMessage.Results`'s `passThrough()`. It can for example hold a `ConsumerMessage.CommittableOffset` or `ConsumerMessage.CommittableOffsetBatch` (from a `Consumer.committableSource`) that can be committed after publishing to Kafka. 


### Produce a single message to Kafka

To create one message to a Kafka topic, use the `ProducerMessage.Message` type as in

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #singleMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #singleMessage }


For flows the `ProducerMessage.Message`s continue as `ProducerMessage.Result` elements containing: 
 
 1. the original input message,
 1. the record metadata (@javadoc[Kafka RecordMetadata API](org.apache.kafka.clients.producer.RecordMetadata)), and
 1. access to the `passThrough` within the message.  


### Let one stream element produce multiple messages to Kafka

The `ProducerMessage.MultiMessage` contains a list of `ProducerRecords` to produce multiple messages to Kafka topics.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #multiMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #multiMessage }

For flows the `ProducerMessage.MultiMessage`s continue as `ProducerMessage.MultiResult` elements containing: 
 
 1. a list of `MultiResultPart` with
    1. the original input message,
    1. the record metadata (@javadoc[Kafka RecordMetadata API](org.apache.kafka.clients.producer.RecordMetadata)), and
 1. the `passThrough` data.  



### Let a stream element pass through, without producing a message to Kafka

The `ProducerMessage.PassThroughMessage` allows to let an element pass through a Kafka flow without producing a new message to a Kafka topic. This is primarily useful with Kafka commit offsets and transactions, so that these can be committed without producing new messages.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #passThroughMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #passThroughMessage }


For flows the `ProducerMessage.PassThroughMessage`s continue as `ProducerMessage.PassThroughResult` elements containing the `passThrough` data.  


## Producer as a Flow

`Producer.flexiFlow` allows the stream to continue after publishing messages to Kafka. It accepts implementations of `ProducerMessage.Envelope` (@scaladoc[API](akka.kafka.ProducerMessage$$Envelope)) as input, which continue in the flow as implementations of `ProducerMessage.Results` (@scaladoc[API](akka.kafka.ProducerMessage$$Results)). 
 

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #flow }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #flow }


The `passThrough` can for example hold a `ConsumerMessage.CommittableOffset` or `ConsumerMessage.CommittableOffsetBatch` that can be committed after publishing to Kafka. 

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #consumerToProducerFlow }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #consumerToProducerFlow }


## Connecting a Producer to a Consumer

See the @ref[Consumer page](consumer.md#connecting-producer-and-consumer).


## Sharing the KafkaProducer instance

The underlying `KafkaProducer` (@javadoc[Kafka API](org.apache.kafka.clients.producer.KafkaProducer)) is thread safe and sharing a single producer instance across streams will generally be faster than having multiple instances.

To create a `KafkaProducer` from the Kafka connector settings described [above](#settings), the `ProducerSettings` contain a factory method `createKafkaProducer`.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #producer }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #producer }

The `KafkaProducer` instance is passed as a parameter to the `Producer` factory methods.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #plainSinkWithProducer }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #plainSinkWithProducer }


## Accessing KafkaProducer metrics

By passing an explicit reference to a `KafkaProducer` (as shown in the previous section) its metrics become accessible. Refer to the @javadoc[Kafka MetricName API](org.apache.kafka.common.MetricName) for more details.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ProducerExample.scala) { #producerMetrics }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #producerMetrics }
