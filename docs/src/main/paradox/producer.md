# Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

The underlying implementation is using the `KafkaProducer`, see the @javadoc[API](org.apache.kafka.clients.producer.KafkaProducer) for details.

## Settings

When creating a producer stream you need to pass in `ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) that define things like:

* bootstrap servers of the Kafka cluster
* serializers for the keys and values
* tuning parameters

Scala
: @@ snip [flow](../../test/scala/sample/scaladsl/ProducerExample.scala) { #settings }

Java
: @@ snip [flow](../../test/java/sample/javadsl/ProducerExample.java) { #settings }

In addition to programmatic construction of the `ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) it can also be created from configuration (`application.conf`). By default when creating `ProducerSettings` with the `ActorSystem` (@scaladoc[API](akka.actor.ActorSystem)) parameter it uses the config section `akka.kafka.producer`.

@@ snip [flow](../../../../core/src/main/resources/reference.conf) { #producer-settings }

`ProducerSettings` (@scaladoc[API](akka.kafka.ProducerSettings)) can also be created from any other `Config` (@scaladoc[API](com.typesafe.config.Config)) section with the same layout as above.

See @javadoc[KafkaProducer API](org.apache.kafka.clients.producer.KafkaProducer) and @javadoc[ProducerConfig API](org.apache.kafka.clients.producer.ProducerConfig) for details.

## Producer as a Sink

`Producer.plainSink` 
(@scala[@scaladoc[Producer API](akka.kafka.scaladsl.Producer)]@java[@scaladoc[Producer API](akka.kafka.javadsl.Producer)]) 
is the easiest way to publish messages. The sink consumes `ProducerRecord` (@javadoc[API](org.apache.kafka.clients.producer.ProducerRecord)) elements which contains a topic name to which the record is being sent, an optional partition number, and an optional key and value.

Scala
: @@ snip [plainSink](../../test/scala/sample/scaladsl/ProducerExample.scala) { #plainSink }
  The materialized value of the sink is a `Future[Done]` which is completed with `Done` when the stream completes or with exception if an error occurs.

Java
: @@ snip [plainSink](../../test/java/sample/javadsl/ProducerExample.java) { #plainSink }
  The materialized value of the sink is a `CompletionStage<Done>` which is completed with `Done` when the stream completes or with exception if an error occurs.

There is another variant of a producer sink named `Producer.commitableSink` that is useful when connecting a consumer to a producer and let the sink commit the offset back to the consumer when it has successfully published the message.

Scala
: @@ snip [consumerToProducerSink](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerSink }

Java
: @@ snip [consumerToProducerSink](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerSink }

Note that there is a risk that something fails after publishing, but before committing, so `commitableSink` has "at-least once delivery" semantics.

## Producer as a Flow

Sometimes there is a need for publishing messages in the middle of the stream processing, not as the last step, and then you can use `Producer.flow`

Scala
: @@ snip [flow](../../test/scala/sample/scaladsl/ProducerExample.scala) { #flow }

Java
: @@ snip [flow](../../test/java/sample/javadsl/ProducerExample.java) { #flow }

It is possible to pass through a message, which can for example be a `ConsumerMessage.CommittableOffset` or `ConsumerMessage.CommittableOffsetBatch` that can be committed later in the flow. Here is an example illustrating that:

Scala
: @@ snip [consumerToProducerFlow](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #consumerToProducerFlow }

Java
: @@ snip [consumerToProducerFlow](../../test/java/sample/javadsl/ConsumerExample.java) { #consumerToProducerFlow }


## Sharing KafkaProducer

If you have many streams it can be more efficient to share the underlying `KafkaProducer` instance.

You can create a `KafkaProducer` instance from `ProducerSettings`.

Scala
: @@ snip [producer](../../test/scala/sample/scaladsl/ProducerExample.scala) { #producer }

Java
: @@ snip [producer](../../test/java/sample/javadsl/ProducerExample.java) { #producer }

The `KafkaProducer` is passed as a parameter to the `Producer` factory methods.

Scala
: @@ snip [plainSinkWithProducer](../../test/scala/sample/scaladsl/ProducerExample.scala) { #plainSinkWithProducer }

Java
: @@ snip [plainSinkWithProducer](../../test/java/sample/javadsl/ProducerExample.java) { #plainSinkWithProducer }


## Accessing KafkaProducer metrics

By passing an explicit reference to a `KafkaProducer` (as shown in the previous section) its metrics become accessible. Refer to the @javadoc[MetricName API](org.apache.kafka.common.MetricName) for more details.

Scala
: @@ snip [plainSinkWithProducer](../../test/scala/sample/scaladsl/ProducerExample.scala) { #producerMetrics }

Java
: @@ snip [plainSinkWithProducer](../../test/java/sample/javadsl/ProducerExample.java) { #producerMetrics }
