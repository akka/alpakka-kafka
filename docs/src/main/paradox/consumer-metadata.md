---
project.description: Access Kafka consumer metadata by sending messages to the actor provided by Alpakka Kafka.
---
# Consumer Metadata

## Metadata Client

`MetadataClient` is a thin wrapper for @apidoc[akka.kafka.KafkaConsumerActor$] hiding the ask calls and mapping to the correct response types.

To access the Kafka consumer metadata you need to create the @apidoc[akka.kafka.KafkaConsumerActor$] as described in the @ref[Consumer documentation](consumer.md#sharing-the-kafkaconsumer-instance) pass it to `MetadataClient`'s factory method `create`.

Another approach to create metadata client is passing the `ConsumerSettings` and `ActorSystem` objects to the factory method. Then the metadata client manages the internal actor and stops it when the `close` method is called.

The metadata the `MetadataClient` provides is documented in the @javadoc[Kafka Consumer API](org.apache.kafka.clients.consumer.KafkaConsumer) API.

## Supported metadata by MetadataClient

The supported metadata are

| Metadata | Response type |
|-------| ------- |
| Topics list | @scala[Future[Map[String, List[PartitionInfo]]]]@java[CompletionStage[java.util.Map[java.lang.String, java.util.List[PartitionInfo]]]] |
| Partitions | @scala[Future[List[PartitionInfo]]]@java[CompletionStage[java.util.List[PartitionInfo]]] |
| Beginning offsets | @scala[Future[Map[TopicPartition, Long]]]@java[CompletionStage[java.util.Map[TopicPartition, java.lang.Long]]] |
| End offsets | @scala[Future[Map[TopicPartition, Long]]]@java[CompletionStage[java.util.Map[TopicPartition, java.lang.Long]]] |
| Committed offset | @scala[Future[OffsetAndMetadata]]@java[CompletionStage[OffsetAndMetadata]] |
   
@@@ warning

Processing of these requests blocks the actor loop. The @apidoc[akka.kafka.KafkaConsumerActor$] is configured to run on its own dispatcher, so just as the other remote calls to Kafka, the blocking happens within a designated thread pool.

However, calling these during consuming might affect performance and even cause timeouts in extreme cases.

Please consider to use a dedicated @apidoc[akka.kafka.KafkaConsumerActor$] to create metadata client requests against.

@@@

Example:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/FetchMetadata.scala) { #metadataClient }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/MetadataClientTest.java) { #metadataClient }


## Accessing metadata using KafkaConsumerActor

To access the Kafka consumer metadata you need to create the @apidoc[akka.kafka.KafkaConsumerActor$] as described in the @ref[Consumer documentation](consumer.md#sharing-the-kafkaconsumer-instance) and send messages from @apidoc[Metadata$] to it.

## Supported metadata by KafkaConsumerActor

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

Accessing the Kafka consumer metadata using the `KafkaConsumerActor` is not a recommended approach. It is reasonable only when you need to perform a request `GetOffsetsForTimes` which is not supported by the `MetadataClient` yet.

@@@

Example:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/FetchMetadata.scala) { #metadata }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/FetchMetadataTest.java) { #metadata }
