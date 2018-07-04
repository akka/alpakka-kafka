# Consumer Metadata

To access the Kafka consumer metadata you need to create the `KafkaConsumerActor` as described in the @ref[Consumer documentation](consumer.md#sharing-the-kafkaconsumer-instance) and send messages from `Metadata` (@scaladoc[API](akka.kafka.Metadata$)) to it.

The metadata the Kafka Consumer provides is documented in the @javadoc[Kafka Consumer API](org.apache.kafka.clients.consumer.KafkaConsumer).

The supported metadata are

| Request | Reply | 
|---------|-------|
| ListTopics | Topics | 
| GetPartitionsFor | PartitionsFor |
| GetBeginningOffsets | BeginningOffsets |
| GetEndOffsets | EndOffsets |
| GetOffsetsForTimes | OffsetsForTimes |
| GetCommittedOffset | CommittedOffset |
   
@@@ warning

Processing of these requests blocks the actor loop. The KafkaConsumerActor is configured to run on its own dispatcher, so just as the other remote calls to Kafka, the blocking happens within a designated thread pool.

However, calling these during consuming might affect performance and even cause timeouts in extreme cases.

@@@   

Scala
: @@ snip [dummy](../../../../tests/src/test/scala/docs//scaladsl/FetchMetadata.scala) { #metadata }

Java
: @@ snip [dummy](../../test/java/sample/javadsl/FetchMetadata.java) { #metadata }
