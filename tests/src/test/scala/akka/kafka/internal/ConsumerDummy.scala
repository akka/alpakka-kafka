/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

trait ConsumerDummy[K, V] extends Consumer[K, V] {
  override def assignment(): java.util.Set[TopicPartition] = ???
  override def subscription(): java.util.Set[String] = ???
  override def subscribe(topics: java.util.Collection[String]): Unit = ???
  override def subscribe(topics: java.util.Collection[String], callback: ConsumerRebalanceListener): Unit = ???
  override def assign(partitions: java.util.Collection[TopicPartition]): Unit = ???
  override def subscribe(pattern: java.util.regex.Pattern, callback: ConsumerRebalanceListener): Unit = ???
  override def subscribe(pattern: java.util.regex.Pattern): Unit = ???
  override def unsubscribe(): Unit = ???
  override def poll(timeout: Long): ConsumerRecords[K, V] = ???
  override def commitSync(): Unit = ???
  override def commitSync(offsets: java.util.Map[TopicPartition, OffsetAndMetadata]): Unit = ???
  override def commitAsync(): Unit = ???
  override def commitAsync(callback: OffsetCommitCallback): Unit = ???
  override def commitAsync(offsets: java.util.Map[TopicPartition, OffsetAndMetadata],
                           callback: OffsetCommitCallback): Unit = ???
  override def seek(partition: TopicPartition, offset: Long): Unit = ???
  override def seekToBeginning(partitions: java.util.Collection[TopicPartition]): Unit = ???
  override def seekToEnd(partitions: java.util.Collection[TopicPartition]): Unit = ???
  override def position(partition: TopicPartition): Long = ???
  override def committed(partition: TopicPartition): OffsetAndMetadata = ???
  override def metrics(): java.util.Map[MetricName, _ <: Metric] = ???
  override def partitionsFor(topic: String): java.util.List[PartitionInfo] = ???
  override def listTopics(): java.util.Map[String, java.util.List[PartitionInfo]] = ???
  override def paused(): java.util.Set[TopicPartition] = ???
  override def pause(partitions: java.util.Collection[TopicPartition]): Unit = ???
  override def resume(partitions: java.util.Collection[TopicPartition]): Unit = ???
  override def offsetsForTimes(
      timestampsToSearch: java.util.Map[TopicPartition, java.lang.Long]
  ): java.util.Map[TopicPartition, OffsetAndTimestamp] = ???
  override def beginningOffsets(
      partitions: java.util.Collection[TopicPartition]
  ): java.util.Map[TopicPartition, java.lang.Long] = ???
  override def endOffsets(
      partitions: java.util.Collection[TopicPartition]
  ): java.util.Map[TopicPartition, java.lang.Long] = ???
  override def close(): Unit = {}
  override def close(timeout: Long, unit: TimeUnit): Unit = {}
  override def wakeup(): Unit = ???
}