# Consumer Metadata

To access the Kafka consumer metadata you need to create the `KafkaConsumerActor` as described in the @ref[Consumer documentation](consumer.md#sharing-the-kafkaconsumer-instance) and send messages from `Metadata` (@scaladoc[API](akka.kafka.Metadata$)) to it.

The metadata the Kafka Consumer provides is documented in the @javadoc[Kafka Consumer API](org.apache.kafka.clients.consumer.KafkaConsumer).

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

These requests are blocking within the Kafka client library up to a timeout configured by `metadata-request-timeout` or `ConsumerSettings.withMetadataRequestTimeout` respectively.
   
@@@ warning

Processing of these requests blocks the actor loop. The `KafkaConsumerActor` is configured to run on its own dispatcher, so just as the other remote calls to Kafka, the blocking happens within a designated thread pool.

However, calling these during consuming might affect performance and even cause timeouts in extreme cases.

Please consider to use a dedicated `KafkaConsumerActor` to run metadata requests against.

@@@   

## Example

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/FetchMetadata.scala) { #metadata }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/FetchMetadataTest.java) { #metadata }
