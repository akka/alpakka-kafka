/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit

import akka.Done
import akka.annotation.ApiMayChange
import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{CommittableOffset, GroupTopicPartition, PartitionOffset}
import akka.kafka.internal.{CommittableOffsetImpl, InternalCommitter}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future

/**
 * Factory methods to create instances that normally are emitted by [[akka.kafka.scaladsl.Consumer]] and [[akka.kafka.javadsl.Consumer]] flows.
 */
@ApiMayChange
object ConsumerResultFactory {

  val fakeCommitter: InternalCommitter = new InternalCommitter {
    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Done] = Future.successful(Done)
  }

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
                        metadata: String): ConsumerMessage.CommittableOffset =
    CommittableOffsetImpl(partitionOffset, metadata)(fakeCommitter)

  def committableMessage[K, V](
      record: ConsumerRecord[K, V],
      committableOffset: CommittableOffset
  ): ConsumerMessage.CommittableMessage[K, V] = ConsumerMessage.CommittableMessage(record, committableOffset)

  def transactionalMessage[K, V](
      record: ConsumerRecord[K, V],
      partitionOffset: PartitionOffset
  ): ConsumerMessage.TransactionalMessage[K, V] = ConsumerMessage.TransactionalMessage(record, partitionOffset)

}
