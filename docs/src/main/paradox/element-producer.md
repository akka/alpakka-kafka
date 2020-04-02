---
project.description: Produce messages to Apache Kafka topics with a simple future API.
---
# Element Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

The Alpakka Kafka @apidoc[ElementProducer] does not integrate with Akka Streams. Instead, it offers a wrapper of the @javadoc[KafkaProducer](org.apache.kafka.clients.producer.KafkaProducer) to send data to Kafka topics in a per-element fashion with a @scala[`Future`-based]@java[`CompletionStage`-based] API.

It supports the same @ref[settings](producer.md#settings) as @apidoc[Producer] and supports @ref[service discovery](discovery.md).

After use, the `Producer` needs to be properly closed via the `close()` method.

## Producing

The Element Producer offers methods for sending

* @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord) with `send`
* @apidoc[ProducerMessage.Envelope] with `sendEnvelope` (similar to `Producer.flexiFlow`)

After use, the Element Producer should be closed with `close()`.

### ProducerRecord

Produce a @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord) to a topic. 

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ElementProducerSpec.scala) { #record }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ElementProducerTest.java) { #record }


### Envelope

The @apidoc[ProducerMessage.MultiMessage] contains a list of @javadoc[ProducerRecord](org.apache.kafka.clients.producer.ProducerRecord)s to produce multiple messages to Kafka topics.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ElementProducerSpec.scala) { #multiMessage }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ElementProducerTest.java) { #multiMessage }

After successful sending, the future completes with a @apidoc[akka.kafka.ProducerMessage.MultiResult] elements containing: 
 
 1. a list of @apidoc[ProducerMessage.MultiResultPart] with
    1. the original input message,
    1. the record metadata (Kafka @javadoc[RecordMetadata](org.apache.kafka.clients.producer.RecordMetadata) API), and
 1. the `passThrough` data.  
