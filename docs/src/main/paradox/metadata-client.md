---
project.description: Access Kafka consumer metadata by calling MetadataClient.
---
# Metadata Client

`MetadataClient` is a thin wrapper for `KafkaConsumerActor` hiding the ask calls and mapping to the correct response types.

To access the Kafka consumer metadata you need to create the `KafkaConsumerActor` as described in the @ref[Consumer documentation](consumer.md#sharing-the-kafkaconsumer-instance) pass it to `MetadataClient`'s factory method `create`.

Another approach to create metadata client is passing the `ConsumerSettings` and `ActorSystem` objects to factory method. Then the metadata client manages the internal actor and stops it when the `close` method is called.  

The metadata the `MetadataClient` provides is documented in the @javadoc[Kafka Consumer API](org.apache.kafka.clients.consumer.KafkaConsumer).

## Supported metadata

The supported metadata are

| Metadata | Response type |
|-------| ------- |
| Topics list | `Future[Map[String, List[PartitionInfo]]]` | 
| Partitions | `Future[List[PartitionInfo]]` |
| Beginning offsets | `Future[Map[TopicPartition, Long]]` |
| End offsets | `Future[Map[TopicPartition, Long]]` |
| Committed offset | `Future[OffsetAndMetadata]` |
   
@@@ warning

Processing of these requests blocks the actor loop. The `KafkaConsumerActor` is configured to run on its own dispatcher, so just as the other remote calls to Kafka, the blocking happens within a designated thread pool.

However, calling these during consuming might affect performance and even cause timeouts in extreme cases.

Please consider to use a dedicated `KafkaConsumerActor` to create metadata client requests against.

@@@   

## Example

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/FetchMetadata.scala) { #metadataClient }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/MetadataClientTest.java) { #metadataClient }
