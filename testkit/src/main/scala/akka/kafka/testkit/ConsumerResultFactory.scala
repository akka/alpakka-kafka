/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit

import akka.Done
import akka.annotation.ApiMayChange
import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{CommittableOffset, GroupTopicPartition, PartitionOffsetCommittedMarker}
import akka.kafka.internal.{CommittableOffsetImpl, KafkaAsyncConsumerCommitterRef}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future

/**
 * Factory methods to create instances that normally are emitted by [[akka.kafka.scaladsl.Consumer]] and [[akka.kafka.javadsl.Consumer]] flows.
 */
@ApiMayChange
object ConsumerResultFactory {

  val fakeCommitter: KafkaAsyncConsumerCommitterRef = new KafkaAsyncConsumerCommitterRef(null, null)(
    ec = scala.concurrent.ExecutionContext.global
  ) {
    private val done = Future.successful(Done)

    override def commitSingle(topicPartition: TopicPartition, offset: OffsetAndMetadata): Future[Done] = done

    override def commitOneOfMulti(topicPartition: TopicPartition, offset: OffsetAndMetadata): Future[Done] = done

    override def tellCommit(topicPartition: TopicPartition, offset: OffsetAndMetadata, emergency: Boolean): Unit = ()
  }

  def partitionOffset(groupId: String, topic: String, partition: Int, offset: Long): ConsumerMessage.PartitionOffset =
    new ConsumerMessage.PartitionOffset(ConsumerMessage.GroupTopicPartition(groupId, topic, partition), offset)

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
      partitionOffset: PartitionOffsetCommittedMarker
  ): ConsumerMessage.TransactionalMessage[K, V] = ConsumerMessage.TransactionalMessage(record, partitionOffset)

}
