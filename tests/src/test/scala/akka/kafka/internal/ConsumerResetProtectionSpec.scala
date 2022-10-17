/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka.OffsetResetProtectionSettings
import akka.kafka.internal.KafkaConsumerActor.Internal.Seek
import akka.kafka.testkit.scaladsl.Slf4jToAkkaLoggingAdapter
import akka.kafka.tests.scaladsl.LogCapturing
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.{Logger, LoggerFactory}

import java.util.Optional
import scala.concurrent.duration._
import scala.collection.JavaConverters._

class ConsumerResetProtectionSpec
    extends TestKit(ActorSystem("ConsumerResetProtectionSpec"))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  val log: Logger = LoggerFactory.getLogger(getClass)
  val adapter: LoggingAdapter = new Slf4jToAkkaLoggingAdapter(log)

  override def afterAll(): Unit = shutdown(system)

  "ConsumerResetProtectionSpec" should {
    val tp = new TopicPartition("tp", 0)
    val m1 = new ConsumerRecord(tp.topic(), tp.partition(), 10L, "k1", "kv")

    def asConsumerRecords[K, V](records: ConsumerRecord[K, V]*): ConsumerRecords[K, V] = {
      new ConsumerRecords[K, V](Map(tp -> records.asJava).asJava)
    }

    val records = asConsumerRecords(m1)

    def shouldHaveEqualRecords[K, V](expected: ConsumerRecords[K, V], actual: ConsumerRecords[K, V]): Unit = {
      expected.count() should be(actual.count())
      expected.iterator().asScala.zip(actual.iterator().asScala).zipWithIndex.foreach {
        case ((expect, act), index) =>
          val fail = s"Mismatch at index $index between expected: \n$expect and actual: \n$act"

          assert(expect.topic.equals(act.topic), fail)
          assert(expect.partition().equals(act.partition), fail)
          assert(expect.key == act.key, fail)
          assert(expect.value == act.value, fail)
          assert(expect.offset.equals(act.offset), fail)
          assert(expect.timestamp.equals(act.timestamp), fail)
      }
    }

    "seek offsets when getting an offset beyond offset threshold" in {
      val progress = new ConsumerProgressTrackerImpl()
      val protection = ConsumerResetProtection(adapter, OffsetResetProtectionSettings(10, 1.day), () => progress)

      progress.assignedPositions(Set(tp), Map(tp -> 100L))
      protection.protect[String, String](self, records).count() should be(0)
      expectMsg(10.seconds, Seek(Map(tp -> 100L)))
    }

    "skip validating offsets when have not received a message yet" in {
      val progress = new ConsumerProgressTrackerImpl()
      val protection = ConsumerResetProtection(adapter, OffsetResetProtectionSettings(10000000, 1.day), () => progress)

      progress.assignedPositions(Set(tp), Map(tp -> 100L))
      protection.protect[String, String](self, records).count() should be(1)
    }

    "seeks offsets when getting beyond a time threshold" in {
      val progress = new ConsumerProgressTrackerImpl()
      val protection =
        ConsumerResetProtection(adapter, OffsetResetProtectionSettings(10000000, 50.millis), () => progress)

      // request offset at 100L
      progress.assignedPositions(Set(tp), Map(tp -> 100L))
      // we have received 100L at timestamp = 100
      progress.received(
        asConsumerRecords(
          new ConsumerRecord(tp.topic(),
                             tp.partition(),
                             /* offset= */ 100L,
                             /* timestamp = */ 100L,
                             TimestampType.LOG_APPEND_TIME,
                             ConsumerRecord.NULL_SIZE,
                             ConsumerRecord.NULL_SIZE,
                             "k1",
                             "kv",
                             new RecordHeaders(),
                             Optional.empty[Integer]())
        )
      )

      // later, we get offset 90L and timestamp 10, the latter of which is outside our 50 milli threshold
      val timeRecords = asConsumerRecords(
        new ConsumerRecord(tp.topic(),
                           tp.partition(),
                           /* offset= */ 90L,
                           /* timestamp = */ 10L,
                           TimestampType.LOG_APPEND_TIME,
                           ConsumerRecord.NULL_SIZE,
                           ConsumerRecord.NULL_SIZE,
                           "k1",
                           "kv",
                           new RecordHeaders(),
                           Optional.empty[Integer]())
      )
      protection.protect[String, String](self, timeRecords).count() should be(0)
      expectMsg(10.seconds, Seek(Map(tp -> 100L)))
    }

    "ignore partitions for which there is no previous assignment" in {
      val progress = new ConsumerProgressTrackerImpl()
      val protection = ConsumerResetProtection(adapter, OffsetResetProtectionSettings(10, 1.day), () => progress)
      // no assignment, records are passed through
      var protectedRecords = protection.protect[String, String](self, records)
      shouldHaveEqualRecords(records, protectedRecords)

      // try assigning and then filtering
      val tp1 = new TopicPartition("tp1", 0)
      progress.assignedPositions(Set(tp1), Map(tp1 -> 100L))
      // there was an assignment, but no applicable offsets in this batch (records is tp, not tp1), so the records are
      // passed through
      protectedRecords = protection.protect[String, String](self, records)
      shouldHaveEqualRecords(records, protectedRecords)
      // drop the old offsets in this batch, so back to the original set of records
      protectedRecords = protection
        .protect(self,
                 new ConsumerRecords(
                   Map(
                     tp -> List(m1).asJava,
                     tp1 -> List(new ConsumerRecord(tp1.topic(), tp1.partition(), 10L, "k1", "kv")).asJava
                   ).asJava
                 ))
      shouldHaveEqualRecords(records, protectedRecords)
    }

    // This is a bit interesting, as we technically allow Kafka to send us records that are outside the allowed
    // threshold. Normally, this would never happen - kafka should only be sending us contiguous blocks of offsets
    // from the offset file. However, it's significantly lower effort to just check first/last of the block, rather
    // than checking every single offset. Even if something like the case below does happen, likely the consumer will
    // commit the tail of the batch, so they will continue to make forward progress; obviously, there is a very rare
    // race in there, but given the protection is there for a rare case itself, should be OK. Later, we can look at
    // supporting a "strict" mode, if it's a problem.
    "just checks the first/last record in the batch" in {
      val progress = new ConsumerProgressTrackerImpl()
      val protection = ConsumerResetProtection(adapter, OffsetResetProtectionSettings(10, 1.day), () => progress)
      progress.assignedPositions(Set(tp), Map(tp -> 100L))
      val records = protection.protect[String, String](
        self,
        new ConsumerRecords(
          Map(
            tp -> List(
              new ConsumerRecord(tp.topic(), tp.partition(), 101L, "k1", "kv"),
              new ConsumerRecord(tp.topic(), tp.partition(), 1L, "k2", "kv"),
              new ConsumerRecord(tp.topic(), tp.partition(), 102L, "k1", "kv")
            ).asJava
          ).asJava
        )
      )
      records.count() should be(3)
      records.records(tp).asScala.map(_.offset()) should be(Seq(101L, 1L, 102L))
    }
  }
}
