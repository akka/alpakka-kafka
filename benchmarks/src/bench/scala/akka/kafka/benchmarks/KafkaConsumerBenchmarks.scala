package akka.kafka.benchmarks

import com.typesafe.scalalogging.LazyLogging
import scala.annotation.tailrec
import scala.collection.JavaConversions._

object KafkaConsumerBenchmarks extends LazyLogging {
  val pollTimeoutMs = 50L

  def consumePlain(fixture: KafkaConsumerTestFixture): Unit = {
    val consumer = fixture.consumer

    @tailrec
    def pollInLoop(readLimit: Int, readSoFar: Int = 0): Int = {
      if (readSoFar >= readLimit)
        readSoFar
      else {
        logger.debug(s"Polling")
        val records = consumer.poll(pollTimeoutMs)
        records.iterator().toList // ensure records are processed
        val recordCount = records.count()
        logger.debug(s"${readSoFar + recordCount} records read. Limit = $readLimit")
        pollInLoop(readLimit, readSoFar + recordCount)
      }
    }

    pollInLoop(readLimit = fixture.msgCount)
    fixture.close()
  }

}
