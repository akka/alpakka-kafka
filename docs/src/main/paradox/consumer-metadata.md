---
project.description: Access Kafka consumer metadata by sending messages to the actor provided by Alpakka Kafka.
---
# Consumer Metadata

To access the Kafka consumer metadata you need to create the @apidoc[akka.kafka.KafkaConsumerActor$] as described in the @ref[Consumer documentation](consumer.md#sharing-the-kafkaconsumer-instance) and send messages from @scaladoc[Metadata](akka.kafka.Metadata$) to it.

The metadata the Kafka Consumer provides is documented in the @javadoc[KafkaConsumer](org.apache.kafka.clients.consumer.KafkaConsumer) API.

## Supported metadata

The supported metadata are

| Request | Reply | 
|---------|-------|
| ListTopics | Topics | 
| GetPartitionsFor | PartitionsFor |
| GetBeginningOffsets | BeginningOffsets |
| GetEndOffsets | EndOffsets |
| GetOffsetsForTimes | OffsetsForTimes |
| GetCommittedOffset | CommittedOffset |

These requests are blocking within the Kafka client library up to a timeout configured by `metadata-request-timeout` or @apidoc[ConsumerSettings.withMetadataRequestTimeout](ConsumerSettings) { java="#withMetadataRequestTimeout(metadataRequestTimeout:java.time.Duration):akka.kafka.ConsumerSettings[K,V]" scala="#withMetadataRequestTimeout(metadataRequestTimeout:scala.concurrent.duration.FiniteDuration):akka.kafka.ConsumerSettings[K,V]" }  respectively.
   
@@@ warning

Processing of these requests blocks the actor loop. The @apidoc[akka.kafka.KafkaConsumerActor$] is configured to run on its own dispatcher, so just as the other remote calls to Kafka, the blocking happens within a designated thread pool.

However, calling these during consuming might affect performance and even cause timeouts in extreme cases.

Please consider to use a dedicated @apidoc[akka.kafka.KafkaConsumerActor$] to run metadata requests against.

@@@   

## Example

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/FetchMetadata.scala) { #metadata }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/FetchMetadataTest.java) { #metadata }
