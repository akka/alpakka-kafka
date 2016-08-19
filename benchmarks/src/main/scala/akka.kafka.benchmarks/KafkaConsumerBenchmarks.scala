/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import java.util
import java.util.concurrent.Semaphore

import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{OffsetCommitCallback, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.JavaConversions._

object KafkaConsumerBenchmarks extends LazyLogging {
  val pollTimeoutMs = 50L

  /**
   * Reads messages from topic in a loop, then discards immediately. Does not commit.
   */
  def consumePlainNoKafka(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int = {
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        meter.mark()
        pollInLoop(readLimit, readSoFar + 1)
      }
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
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int = {
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        val records = consumer.poll(pollTimeoutMs)
        val recordCount = records.count()
        records.iterator().toList // ensure records are processed
        meter.mark(recordCount.toLong)
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }
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

    var lastProcessedOffset = 0L
    var accumulatedMsgCount = 0L
    // TODO this is actually running single threaded, commitAsync is called on the poll thread.
    //      It might not be waiting as it should for the commits to be completed?
    val semaphore = new Semaphore(1)

    def doCommit(): Unit = {
      semaphore.acquire()
      accumulatedMsgCount = 0
      val offsetMap = Map(new TopicPartition(fixture.topic, 0) -> new OffsetAndMetadata(lastProcessedOffset))
      logger.debug("Committing offset " + offsetMap.head._2.offset())
      consumer.commitAsync(offsetMap, new OffsetCommitCallback {
        override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = {
          semaphore.release()
        }
      })
    }

    def noCommitInProgress: Boolean = semaphore.availablePermits() > 0

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int = {
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug("Polling")
        val records = consumer.poll(pollTimeoutMs)
        for (record <- records.iterator()) {
          accumulatedMsgCount = accumulatedMsgCount + 1
          meter.mark()
          lastProcessedOffset = record.offset()
          if (accumulatedMsgCount >= batchSize || noCommitInProgress)
            doCommit()
        }

        val recordCount = records.count()
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }
    }

    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }

  /**
   * Reads messages from topic in a loop and commits each single message.
   */
  def consumeCommitAtMostOnce(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {
    val consumer = fixture.consumer
    val semaphore = new Semaphore(1)
    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int = {
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        val records = consumer.poll(pollTimeoutMs)

        for (record <- records.iterator()) {
          meter.mark()
          val offsetMap = Map(new TopicPartition(fixture.topic, 0) -> new OffsetAndMetadata(record.offset()))
          semaphore.acquire()
          consumer.commitAsync(offsetMap, new OffsetCommitCallback {
            override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = {
              semaphore.release()
            }
          })
        }

        val recordCount = records.count()
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }
    }

    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }
}
