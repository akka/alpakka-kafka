/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit

import java.util.concurrent.CompletionStage

import akka.Done
import akka.annotation.ApiMayChange
import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{CommittableOffset, GroupTopicPartition, PartitionOffset}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future
import scala.compat.java8.FutureConverters._

/**
 * Factory methods to create instances that normally are emitted by [[akka.kafka.scaladsl.Consumer]] and [[akka.kafka.javadsl.Consumer]] flows.
 */
@ApiMayChange
object ConsumerResultFactory {

  def partitionOffset(groupId: String, topic: String, partition: Int, offset: Long): ConsumerMessage.PartitionOffset =
    ConsumerMessage.PartitionOffset(ConsumerMessage.GroupTopicPartition(groupId, topic, partition), offset)

  def partitionOffset(key: GroupTopicPartition, offset: Long) = ConsumerMessage.PartitionOffset(key, offset)

  def committableOffset(groupId: String,
                        topic: String,
                        partition: Int,
                        offset: Long,
                        metadata: String): ConsumerMessage.CommittableOffset =
    committableOffset(partitionOffset(groupId, topic, partition, offset), metadata)

  def committableOffset(partitionOffset: ConsumerMessage.PartitionOffset,
                        metadata: String): ConsumerMessage.CommittableOffset = {
    val metadata2 = metadata
    val partitionOffset2 = partitionOffset
    new ConsumerMessage.CommittableOffsetMetadata {
      override val metadata: String = metadata2
      override val partitionOffset: ConsumerMessage.PartitionOffset = partitionOffset2
      override def commitScaladsl(): Future[Done] = Future.successful(Done)
      override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava
    }
  }

  def committableMessage[K, V](
      record: ConsumerRecord[K, V],
      committableOffset: CommittableOffset
  ): ConsumerMessage.CommittableMessage[K, V] = ConsumerMessage.CommittableMessage(record, committableOffset)

  def transactionalMessage[K, V](
      record: ConsumerRecord[K, V],
      partitionOffset: PartitionOffset
  ): ConsumerMessage.TransactionalMessage[K, V] = ConsumerMessage.TransactionalMessage(record, partitionOffset)

}
