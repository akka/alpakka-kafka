# Subscription

Consumer Sources are created with different types of subscriptions, which control from which topics, partitions and offsets data is to be consumed.

Subscriptions are grouped into two categories: with automatic partition assignment and with manual control of partition assignment.

Factory methods for all subscriptions can be found in the @scaladoc[Subscriptions](akka.kafka.Subscriptions$) factory.

## Automatic Partition Assignment

### Topic

Subscribes to one or more topics. Partitions will be assigned automatically by the Kafka Client.

### Topic Pattern

Subscribes to one or more topics which match the given pattern. Take a look at the @javadoc[`subscribe​(java.util.regex.Pattern pattern,
...)`](org.apache.kafka.clients.consumer.KafkaConsumer) method documentation for more information on topic pattern matching.

## Manual Partition Assignment

### Partition Assignment

Subscribes to given topics and their given partitions.

### Partition Assignment with Offset

Subscribes to given topics and their partitions allowing to also set an offset from which messages will be read.

This subscription can be used when offsets are stored in Kafka or on external storage. For more information, take a look at the @ref[Offset Storage external to Kafka](consumer.md#offset-storage-external-to-kafka) documentation page.

### Partition Assignment with Timestamp

Subscribes to given topics and their partitions allowing to also set a timestamp which will be used to find the offset from which messages will be read.
