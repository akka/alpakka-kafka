# Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

The underlying implementation is using the `KafkaProducer`, see the @javadoc[Kafka API](org.apache.kafka.clients.producer.KafkaProducer) for details.

## Settings

When creating a producer stream you need to pass in `ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) that define things like:

* bootstrap servers of the Kafka cluster
* serializers for the keys and values
* tuning parameters

Scala
: @@ snip [flow](../../test/scala/sample/scaladsl/ProducerExample.scala) { #settings }

Java
: @@ snip [flow](../../test/java/sample/javadsl/ProducerExample.java) { #settings }

In addition to programmatic construction of the `ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) it can also be created from configuration (`application.conf`). 

When creating `ProducerSettings` with the `ActorSystem` (@scaladoc[API](akka.actor.ActorSystem)) settings it uses the config section `akka.kafka.producer`. The format of these settings files are described in the [Typesafe Config Documentation](https://github.com/lightbend/config#using-hocon-the-json-superset).

@@ snip [flow](../../../../core/src/main/resources/reference.conf) { #producer-settings }

`ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) can also be created from any other `Config` section with the same layout as above.

See @javadoc[KafkaProducer API](org.apache.kafka.clients.producer.KafkaProducer) and @javadoc[ProducerConfig API](org.apache.kafka.clients.producer.ProducerConfig) for more details regarding settings.

## Producer as a Sink

`Producer.plainSink` 
(@scala[@scaladoc[Producer API](akka.kafka.scaladsl.Producer)]@java[@scaladoc[Producer API](akka.kafka.javadsl.Producer)]) 
is the easiest way to publish messages. The sink consumes the Kafka type `ProducerRecord` (@javadoc[API](org.apache.kafka.clients.producer.ProducerRecord)) which contains 

1. a topic name to which the record is being sent, 
1. an optional partition number, 
1. an optional key, 
1. and a value.

Scala
: @@ snip [plainSink](../../test/scala/sample/scaladsl/ProducerExample.scala) { #plainSink }
  The materialized value of the sink is a `Future[Done]` which is completed with `Done` when the stream completes, or with with an exception in case an error occurs.

Java
: @@ snip [plainSink](../../test/java/sample/javadsl/ProducerExample.java) { #plainSink }
  The materialized value of the sink is a `CompletionStage<Done>` which is completed with `Done` when the stream completes, or with an exception in case an error occurs.

### Connecting a Producer to a Consumer

For simple cases of transferring from Kafka topics to another Kafka topic the `Producer.commitableSink` is useful, as this sink commits the offset back to the Kafka consumer, when it successfully published the message.

The `committableSink` accepts messages of type `ProducerMessage.Message` (@scaladoc[API](akka.kafka.ProducerMessage$$Message)) to combine Kafka's `ProducerRecord` with the offset to commit the consumption of the originating message (of type `ConsumerMessage.Committable` (@scaladoc[API](akka.kafka.ConsumerMessage$$Committable))).

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerSink }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerSink }

@@@note 

There is a risk that something fails after publishing, but before committing, so `commitableSink` has "at-least once delivery" semantics.

To get delivery guarantees, please read about @ref[transactions](transactions.md).

@@@


## Producer as a Flow

`Producer.flow` allows the stream to continue after publishing messages to Kafka. It uses type `ProducerMessage.Message` (@scaladoc[API](akka.kafka.ProducerMessage$$Message)) as input, and type `ProducerMessage.Result` (@scaladoc[API](akka.kafka.ProducerMessage$$Result)) as output. 

The `ProducerMessage.Result` contains
 
 1. the original input message, and
 1. the record metadata (@javadoc[Kafka RecordMetadata API](org.apache.kafka.clients.producer.RecordMetadata)).  

Scala
: @@ snip [flow](../../test/scala/sample/scaladsl/ProducerExample.scala) { #flow }

Java
: @@ snip [flow](../../test/java/sample/javadsl/ProducerExample.java) { #flow }

### Passing data past a producer

The `ProducerMessage.Message` (@scaladoc[API](akka.kafka.ProducerMessage$$Message)) contains an extra field to pass through data, the so called `passThrough`. Its value is passed through the flow and becomes available in the `ProducerMessage.Result`'s `message().passThrough()` as the result contains the whole incoming message.
  
It can for example hold a `ConsumerMessage.CommittableOffset` or `ConsumerMessage.CommittableOffsetBatch` that can be committed after publishing to Kafka. 

Scala
: @@ snip [consumerToProducerFlow](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerFlow }

Java
: @@ snip [consumerToProducerFlow](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerFlow }


## Sharing the KafkaProducer instance

The underlying `KafkaProducer` (@javadoc[Kafka API](org.apache.kafka.clients.producer.KafkaProducer)) is thread safe and sharing a single producer instance across streams will generally be faster than having multiple instances.

To create a `KafkaProducer` from the Kafka connector settings described [above](#settings), the `ProducerSettings` contain a factory method `createKafkaProducer`.

Scala
: @@ snip [producer](../../test/scala/sample/scaladsl/ProducerExample.scala) { #producer }

Java
: @@ snip [producer](../../test/java/sample/javadsl/ProducerExample.java) { #producer }

The `KafkaProducer` instance is passed as a parameter to the `Producer` factory methods.

Scala
: @@ snip [plainSinkWithProducer](../../test/scala/sample/scaladsl/ProducerExample.scala) { #plainSinkWithProducer }

Java
: @@ snip [plainSinkWithProducer](../../test/java/sample/javadsl/ProducerExample.java) { #plainSinkWithProducer }


## Accessing KafkaProducer metrics

By passing an explicit reference to a `KafkaProducer` (as shown in the previous section) its metrics become accessible. Refer to the @javadoc[Kafka MetricName API](org.apache.kafka.common.MetricName) for more details.

Scala
: @@ snip [plainSinkWithProducer](../../test/scala/sample/scaladsl/ProducerExample.scala) { #producerMetrics }

Java
: @@ snip [plainSinkWithProducer](../../test/java/sample/javadsl/ProducerExample.java) { #producerMetrics }
