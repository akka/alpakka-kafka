/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import akka.annotation.InternalApi
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

/**
 * A buffer of messages provided by the [[KafkaConsumerActor]] for a Source Logic. When partitions are rebalanced
 * away from this Source Logic preemptively filter out messages for those partitions.
 *
 * NOTE: Due to the asynchronous nature of Akka Streams, it's not possible to guarantee that a message has not
 * already been sent downstream for a revoked partition before the rebalance handler invokes
 * `filterRevokedPartitionsCB`. The best we can do is filter as many messages as possible to reduce the amount of
 * duplicate messages sent downstream.
 */
@InternalApi
private[kafka] trait SourceLogicBuffer[K, V, Msg] {
  self: GraphStageLogic with StageIdLogging =>

  protected var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  protected val filterRevokedPartitionsCB: AsyncCallback[Set[TopicPartition]] =
    getAsyncCallback[Set[TopicPartition]](filterRevokedPartitions)

  private def filterRevokedPartitions(topicPartitions: Set[TopicPartition]): Unit = {
    if (topicPartitions.nonEmpty) {
      log.debug("Filtering out messages from revoked partitions {}", topicPartitions)
      // as buffer is an Iterator the filtering will be applied during `pump`
      buffer = buffer.filterNot { record =>
        val tp = new TopicPartition(record.topic, record.partition)
        val filtered = topicPartitions.contains(tp)
        if (filtered)
          log.debug("Filtering offset {} on topic partition {} value {}", record.offset(), tp, record.value())
        filtered
      }
    }
  }
}
