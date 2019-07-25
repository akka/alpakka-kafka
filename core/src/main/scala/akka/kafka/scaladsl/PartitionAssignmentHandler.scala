/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.annotation.ApiMayChange
import akka.kafka.RestrictedConsumer
import org.apache.kafka.common.TopicPartition

/**
 * The API is new and may change in further releases.
 *
 * Allows to execute user code when Kafka rebalances partitions between consumers, or an Alpakka Kafka consumer is stopped.
 * Use with care: These callbacks are called synchronously on the same thread Kafka's `poll()` is called.
 * A warning will be logged if a callback takes longer than the configured `partition-handler-warning`.
 *
 * There is no point in calling `CommittableOffset`'s commit methods as their committing won't be executed as long as any of
 * the callbacks in this class are called.
 *
 * This complements the methods of Kafka's [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener ConsumerRebalanceListener]] with
 * an `onStop` callback.
 */
@ApiMayChange
trait PartitionAssignmentHandler {

  /**
   * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked]]
   *
   * @param revokedTps The list of partitions that were assigned to the consumer on the last rebalance
   * @param consumer A restricted version of the internally used [[org.apache.kafka.clients.consumer.Consumer Consumer]]
   */
  def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit

  /**
   * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned]]
   *
   * @param assignedTps The list of partitions that are now assigned to the consumer (may include partitions previously assigned to the consumer)
   * @param consumer A restricted version of the internally used [[org.apache.kafka.clients.consumer.Consumer Consumer]]
   */
  def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit

  /**
   * Called before a consumer is closed.
   * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked]]
   *
   * @param revokedTps The list of partitions that are currently assigned to the consumer
   * @param consumer A restricted version of the internally used [[org.apache.kafka.clients.consumer.Consumer Consumer]]
   */
  def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit
}
