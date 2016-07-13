package akka.kafka.benchmarks

import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.JavaConversions._

object KafkaConsumerBenchmarks extends LazyLogging {
  val pollTimeoutMs = 50L

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
    * Reads messages from topic in a loop and groups in batches of given size. Once a batch is completed, commits
    * synchronously and then discards the batch.
    */
  def consumerAtLeastOnceBatched(batchSize: Int)(fixture: KafkaConsumerTestFixture, meter: Meter): Unit = {
    val consumer = fixture.consumer

    var lastProcessedOffset = 0L
    var accumulatedMsgCount = 0L

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int = {
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        val records = consumer.poll(pollTimeoutMs)

        for (record <- records.iterator()) {
          accumulatedMsgCount = accumulatedMsgCount + 1
          lastProcessedOffset = record.offset()
          meter.mark()
          if (accumulatedMsgCount >= batchSize) {
            logger.debug(s"Committing a batch of $batchSize messages")
            val offsetMap = Map(new TopicPartition(fixture.topic, 0) -> new OffsetAndMetadata(lastProcessedOffset))
            consumer.commitSync(offsetMap)
            accumulatedMsgCount = 0
          }
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
          consumer.commitSync(offsetMap)
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
