/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Promise

object ConsumerDummy {
  val instanceCounter = new AtomicInteger(0)

  trait TpState
  object Assigned extends TpState
  object Revoked extends TpState
  object Paused extends TpState
  object Resumed extends TpState
}

trait ConsumerDummy[K, V] extends Consumer[K, V] {
  def name: String
  lazy val log: Logger = LoggerFactory.getLogger(name)

  private val firstPausingPromise = Promise[Done]()
  def started = firstPausingPromise.future

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
  override def pause(partitions: java.util.Collection[TopicPartition]): Unit =
    firstPausingPromise.trySuccess(Done)
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

  override def commitSync(timeout: java.time.Duration): Unit = ???
  override def commitSync(offsets: java.util.Map[TopicPartition, OffsetAndMetadata],
                          timeout: java.time.Duration): Unit = ???
  override def position(partition: TopicPartition, timeout: java.time.Duration): Long = ???
  override def committed(partition: TopicPartition, timeout: java.time.Duration): OffsetAndMetadata = ???
  override def partitionsFor(topic: String, timeout: java.time.Duration): java.util.List[PartitionInfo] = ???
  override def listTopics(timeout: java.time.Duration): java.util.Map[String, java.util.List[PartitionInfo]] = ???
  override def offsetsForTimes(timestampsToSearch: java.util.Map[TopicPartition, java.lang.Long],
                               timeout: java.time.Duration): java.util.Map[TopicPartition, OffsetAndTimestamp] = ???
  override def beginningOffsets(partitions: java.util.Collection[TopicPartition],
                                timeout: java.time.Duration): java.util.Map[TopicPartition, java.lang.Long] = ???
  override def endOffsets(partitions: java.util.Collection[TopicPartition],
                          timeout: java.time.Duration): java.util.Map[TopicPartition, java.lang.Long] = ???
  override def close(timeout: java.time.Duration): Unit = ???
  override def poll(timeout: java.time.Duration): ConsumerRecords[K, V] = ???

}
