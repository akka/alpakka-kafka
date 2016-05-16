/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka

/**
 * Offset position for a clientId, topic, partition.
 */
final case class PartitionOffset(key: ClientTopicPartition, offset: Long)

/**
 * clientId, topic, partition key for an offset position.
 */
final case class ClientTopicPartition(
  clientId: String,
  topic: String,
  partition: Int
)
