---
project.description: Produce messages to Apache Kafka topics with a Java or Scala future API.
---
# Send Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

The Alpakka Kafka @apidoc[SendProducer] does not integrate with Akka Streams. Instead, it offers a wrapper of the Apache Kafka @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) to send data to Kafka topics in a per-element fashion with a @scala[`Future`-based]@java[`CompletionStage`-based] API.

It supports the same @ref[settings](producer.md#settings) as Alpakka @apidoc[Producer] flows and sinks and supports @ref[service discovery](discovery.md).

After use, the `Producer` needs to be properly closed via the asynchronous `close()` method.

## Producing

The Send Producer offers methods for sending

* @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord) with `send`
* @apidoc[ProducerMessage.Envelope] with `sendEnvelope` (similar to `Producer.flexiFlow`)

After use, the Send Producer should be closed with `close()`.

### ProducerRecord

Produce a @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord) to a topic.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/SendProducerSpec.scala) { #record }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/SendProducerTest.java) { #record }


### Envelope

The @apidoc[ProducerMessage.Envelope] can be used to send one record, or a list of of @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord)s to produce a single or multiple messages to Kafka topics. The envelope can be used to pass through an arbitrary value which will be attached to the result.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/SendProducerSpec.scala) { #multiMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/SendProducerTest.java) { #multiMessage }

After successful sending, a @apidoc[ProducerMessage.Message] will return a @apidoc[akka.kafka.ProducerMessage.Result] element containing:

 1. the original input message,
 1. the record metadata (Kafka @javadoc[RecordMetadata](org.apache.kafka.clients.producer.RecordMetadata) API), and
 1. access to the `passThrough` within the message.

A @apidoc[ProducerMessage.MultiMessage] will return a @apidoc[akka.kafka.ProducerMessage.MultiResult] containing:

 1. a list of @apidoc[ProducerMessage.MultiResultPart] with
    1. the original input message,
    1. the record metadata (Kafka @javadoc[RecordMetadata](org.apache.kafka.clients.producer.RecordMetadata) API), and
 1. the `passThrough` data.
