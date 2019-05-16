/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.KafkaConsumerBenchmarks.pollTimeoutMs
import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object KafkaTransactionBenchmarks extends LazyLogging {

  /**
   * Process records in a consume-transform-produce transactional workflow and commit every interval.
   */
  def consumeTransformProduceTransaction(commitInterval: FiniteDuration)(fixture: KafkaTransactionTestFixture,
                                                                         meter: Meter): Unit = {
    val consumer = fixture.consumer
    val producer = fixture.producer
    val msgCount = fixture.msgCount
    val logPercentStep = 1
    val loggedStep = if (msgCount > logPercentStep) 100 else 1

    logger.debug(s"Transaction commit interval: ${commitInterval.toMillis}ms")

    var lastProcessedOffset = 0L
    var accumulatedMsgCount = 0L
    var lastCommit = 0L

    def doCommit(): Unit = {
      accumulatedMsgCount = 0
      val offsetMap = Map(new TopicPartition(fixture.sourceTopic, 0) -> new OffsetAndMetadata(lastProcessedOffset))
      logger.debug("Committing offset " + offsetMap.head._2.offset())
      producer.sendOffsetsToTransaction(offsetMap.asJava, fixture.groupId)
      producer.commitTransaction()
    }

    def beginTransaction(): Unit = {
      logger.debug("Beginning transaction")
      lastCommit = System.nanoTime()
      producer.beginTransaction()
    }

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int =
      if (readSoFar >= readLimit) {
        doCommit()
        readSoFar
      } else {
        logger.debug("Polling")
        val records = consumer.poll(pollTimeoutMs)
        for (record <- records.iterator().asScala) {
          accumulatedMsgCount = accumulatedMsgCount + 1
          lastProcessedOffset = record.offset()

          val producerRecord = new ProducerRecord(fixture.sinkTopic, record.partition(), record.key(), record.value())
          producer.send(producerRecord, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = meter.mark()
          })
          if (lastProcessedOffset % loggedStep == 0)
            logger.info(
              s"Transformed $lastProcessedOffset elements to Kafka (${100 * lastProcessedOffset / msgCount}%)"
            )

          if (System.nanoTime() >= lastCommit + commitInterval.toNanos) {
            doCommit()
            beginTransaction()
          }
        }
        val recordCount = records.count()
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }

    meter.mark()
    logger.debug("Initializing transactions")
    producer.initTransactions()
    beginTransaction()
    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }
}
