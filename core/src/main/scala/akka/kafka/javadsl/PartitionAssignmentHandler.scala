/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.javadsl

import akka.kafka.RestrictedConsumer
import org.apache.kafka.common.TopicPartition

/**
 * The API is new and may change in further releases.
 *
 * Allows the user to execute user code when Kafka rebalances partitions between consumers, or an Alpakka Kafka consumer is stopped.
 * Use with care: These callbacks are called synchronously on the same thread Kafka's `poll()` is called.
 * A warning will be logged if a callback takes longer than the configured `partition-handler-warning`.
 *
 * There is no point in calling `Committable`'s commit methods as their committing won't be executed as long as any of
 * the callbacks in this class are called. Calling `commitSync` on the passed [[akka.kafka.RestrictedConsumer]] is available.
 *
 * This complements the methods of Kafka's [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener ConsumerRebalanceListener]] with
 * an `onStop` callback which is called before `Consumer.close`.
 */
trait PartitionAssignmentHandler {

  /**
   * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked]]
   *
   * @param revokedTps The list of partitions that were revoked from the consumer
   * @param consumer The [[akka.kafka.RestrictedConsumer]] gives some access to the internally used [[org.apache.kafka.clients.consumer.Consumer Consumer]]
   */
  def onRevoke(revokedTps: java.util.Set[TopicPartition], consumer: RestrictedConsumer): Unit

  /**
   * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned]]
   *
   * @param assignedTps The list of partitions that are now assigned to the consumer (may include partitions previously assigned to the consumer)
   * @param consumer The [[akka.kafka.RestrictedConsumer]] gives some access to the internally used [[org.apache.kafka.clients.consumer.Consumer Consumer]]
   */
  def onAssign(assignedTps: java.util.Set[TopicPartition], consumer: RestrictedConsumer): Unit

  /**
   * Called when partition metadata has changed and partitions no longer exist.  This can occur if a topic is deleted or if the leader's metadata is stale.
   * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost]]
   *
   * @param lostTps The list of partitions that are no longer valid
   * @param consumer The [[akka.kafka.RestrictedConsumer]] gives some access to the internally used [[org.apache.kafka.clients.consumer.Consumer Consumer]]
   */
  def onLost(lostTps: java.util.Set[TopicPartition], consumer: RestrictedConsumer): Unit

  /**
   * Called before a consumer is closed.
   * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked]]
   *
   * @param currentTps The list of partitions that are currently assigned to the consumer
   * @param consumer The [[akka.kafka.RestrictedConsumer]] gives some access to the internally used [[org.apache.kafka.clients.consumer.Consumer Consumer]]
   */
  def onStop(currentTps: java.util.Set[TopicPartition], consumer: RestrictedConsumer): Unit

}
