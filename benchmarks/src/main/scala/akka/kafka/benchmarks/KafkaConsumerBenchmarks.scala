/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.time.Duration
import java.util

import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object KafkaConsumerBenchmarks extends LazyLogging {
  val pollTimeoutMs: Duration = Duration.ofMillis(50L)

  /**
   * Reads messages from topic in a loop, then discards immediately. Does not commit.
   */
  def consumePlainNoKafka(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int =
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        meter.mark()
        pollInLoop(readLimit, readSoFar + 1)
      }
    meter.mark()
    pollInLoop(readLimit = fixture.msgCount)
  }

  /**
   * Reads messages from topic in a loop, then discards immediately. Does not commit.
   */
  def consumePlain(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {
    val consumer = fixture.consumer

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int =
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        val records = consumer.poll(pollTimeoutMs)
        val recordCount = records.count()
        records.iterator().asScala.toList // ensure records are processed
        meter.mark(recordCount.toLong)
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }
    meter.mark()
    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }

  /**
   * Reads messages from topic in a loop and groups in batches of given max size. Once a batch is completed,
   * batches the next part.
   */
  def consumerAtLeastOnceBatched(batchSize: Int)(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {
    val consumer = fixture.consumer

    var lastProcessedOffset = Map.empty[Int, Long]
    var accumulatedMsgCount = 0L
    var commitInProgress = false
    val assignment = consumer.assignment()

    def doCommit(): Unit = {
      accumulatedMsgCount = 0
      val offsetMap = lastProcessedOffset.map {
        case (partition, offset) =>
          new TopicPartition(fixture.topic, partition) -> new OffsetAndMetadata(offset)
      }
      logger.debug("Committing offset " + offsetMap.head._2.offset())
      consumer.commitAsync(
        offsetMap.asJava,
        new OffsetCommitCallback {
          override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit =
            commitInProgress = false
        }
      )
      lastProcessedOffset = Map.empty[Int, Long]
    }

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int =
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug("Polling")
        if (!commitInProgress)
          consumer.resume(assignment)
        val records = consumer.poll(pollTimeoutMs)
        for (record <- records.iterator().asScala) {
          accumulatedMsgCount = accumulatedMsgCount + 1
          meter.mark()
          lastProcessedOffset += record.partition() -> record.offset()
          if (accumulatedMsgCount >= batchSize) {
            if (!commitInProgress) {
              commitInProgress = true
              doCommit()
            } else // previous commit still in progress
              consumer.pause(assignment)
          }
        }
        val recordCount = records.count()
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }

    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }

  /**
   * Reads messages from topic in a loop and commits all read offsets.
   */
  def consumerAtLeastOnceCommitEveryPoll()(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {
    val consumer = fixture.consumer

    var lastProcessedOffset = Map.empty[Int, Long]

    def doCommit(): Unit = {
      val offsetMap = lastProcessedOffset.map {
        case (partition, offset) =>
          new TopicPartition(fixture.topic, partition) -> new OffsetAndMetadata(offset)
      }
      logger.debug("Committing offset " + offsetMap.head._2.offset())
      consumer.commitAsync(
        offsetMap.asJava,
        new OffsetCommitCallback {
          override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = ()
        }
      )
      lastProcessedOffset = Map.empty[Int, Long]
    }

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int =
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug("Polling")
        val records = consumer.poll(pollTimeoutMs)
        for (record <- records.iterator().asScala) {
          meter.mark()
          lastProcessedOffset += record.partition() -> record.offset()
        }
        doCommit()
        val recordCount = records.count()
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }

    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }

  /**
   * Reads messages from topic in a loop and commits each single message.
   */
  def consumeCommitAtMostOnce(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {
    val consumer = fixture.consumer
    val assignment = consumer.assignment()
    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int =
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        consumer.pause(assignment)
        val records = consumer.poll(pollTimeoutMs)

        for (record <- records.iterator().asScala) {
          meter.mark()
          val offsetMap = Map(new TopicPartition(fixture.topic, 0) -> new OffsetAndMetadata(record.offset()))
          consumer.pause(assignment)

          consumer.commitAsync(
            offsetMap.asJava,
            new OffsetCommitCallback {
              override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit =
                consumer.resume(assignment)
            }
          )
        }

        val recordCount = records.count()
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }

    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }
}
