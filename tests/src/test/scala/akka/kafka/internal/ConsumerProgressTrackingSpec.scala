/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.kafka.tests.scaladsl.LogCapturing
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ConsumerProgressTrackingSpec extends AnyFlatSpecLike with Matchers with LogCapturing {

  private val tp = new TopicPartition("t", 0)
  private val m1 = new ConsumerRecord[String, String](tp.topic(), tp.partition(), 10L, "k1", "kv")
  def asConsumerRecords[K, V](tp: TopicPartition, records: ConsumerRecord[K, V]*): ConsumerRecords[K, V] = {
    new ConsumerRecords[K, V](Map(tp -> records.asJava).asJava)
  }
  private val records = asConsumerRecords(tp, m1)

  it should "add requested offsets" in {
    val tracker = new ConsumerProgressTrackerImpl()
    tracker.requestedOffsets shouldBe empty
    tracker.committedOffsets shouldBe empty
    tracker.requested(Map(tp -> offset(0)))
    tracker.requestedOffsets should have size 1
    tracker.requestedOffsets(tp).offset() should be(0)
    tracker.receivedMessages shouldBe empty
    tracker.committedOffsets shouldBe empty
  }

  it should "track received records" in {
    val tracker = new ConsumerProgressTrackerImpl()
    tracker.requested(Map(tp -> offset(0)))
    tracker.received(records)

    tracker.requestedOffsets(tp).offset() should be(0)
    tracker.receivedMessages should have size 1
    tracker.receivedMessages(tp).offset should be(10L)
    tracker.receivedMessages(tp).timestamp should be(ConsumerRecord.NO_TIMESTAMP)
  }

  it should "skip tracking received records that are not assigned" in {
    val tracker = new ConsumerProgressTrackerImpl()

    tracker.received(asConsumerRecords(tp, m1))

    tracker.requestedOffsets shouldBe empty
    tracker.receivedMessages shouldBe empty

    val tp2 = new TopicPartition("t", 2)
    tracker.requested(Map(tp2 -> offset(0)))
    tracker.received(asConsumerRecords(tp, m1))
    tracker.requestedOffsets should have size 1
    tracker.receivedMessages shouldBe empty
  }

  it should "track offsets when they are committed" in {
    val tracker = new ConsumerProgressTrackerImpl()
    tracker.requestedOffsets shouldBe empty
    tracker.committedOffsets shouldBe empty
    val tp = new TopicPartition("t", 0)
    tracker.requested(Map(tp -> offset(0)))
    tracker.committed(Map(tp -> offset(1)).asJava)
    // requested shouldn't change, just the committed
    tracker.requestedOffsets(tp).offset() should be(0)
    tracker.committedOffsets(tp).offset() should be(1)
  }

  it should "not overwrite existing committed offsets when assigning" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val tp = new TopicPartition("t", 0)
    tracker.committed(Map(tp -> offset(1)).asJava)
    tracker.assignedPositions(Set(tp), Map(tp -> 2L))
    tracker.committedOffsets(tp).offset() should be(1)
  }

  it should "revoke assigned offsets" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val tp1 = new TopicPartition("t", 1)
    tracker.requested(Map(tp -> offset(0), tp1 -> offset(1)))
    tracker.received(records)
    tracker.committed(Map(tp -> offset(1)).asJava)
    tracker.revoke(Set(tp))
    tracker.requestedOffsets should have size 1
    tracker.requestedOffsets(tp1).offset() should be(1)
    tracker.receivedMessages shouldBe empty
    tracker.committedOffsets shouldBe empty
  }

  it should "directly use assigned positions" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val tp0 = new TopicPartition("t", 0)
    val tp1 = new TopicPartition("t", 1)
    tracker.assignedPositions(Set(tp0, tp1), Map(tp0 -> 0L, tp1 -> 1L))
    tracker.requestedOffsets(tp0).offset() should be(0)
    tracker.requestedOffsets(tp1).offset() should be(1)
    tracker.committedOffsets(tp0).offset() should be(0)
    tracker.committedOffsets(tp1).offset() should be(1)
  }

  it should "lookup positions from the consumer when assigned" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val consumer = Mockito.mock(classOf[Consumer[AnyRef, AnyRef]])
    val tp0 = new TopicPartition("t", 0)
    val duration = java.time.Duration.ofSeconds(10)
    Mockito.when(consumer.position(tp0, duration)).thenReturn(5)
    tracker.assignedPositionsAndSeek(Set(tp0), consumer, duration)
    tracker.requestedOffsets(tp0).offset() should be(5)
    tracker.committedOffsets(tp0).offset() should be(5)
  }

  it should "pass through requests to listeners" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val listener = new ConsumerAssignmentTrackingListener {
      var state = Map[TopicPartition, Long]()
      override def revoke(revokedTps: Set[TopicPartition]): Unit = {
        state = state.filter { case (tp, _) => !revokedTps.contains(tp) }
      }
      override def assignedPositions(assignedTps: Set[TopicPartition],
                                     assignedOffsets: Map[TopicPartition, Long]): Unit = {
        state = state ++ assignedOffsets
      }
    }
    tracker.addProgressTrackingCallback(listener)

    def verifyOffsets(offsets: Map[TopicPartition, Long]): Unit = {
      listener.state should be(offsets)
    }

    // assign
    val consumer = Mockito.mock(classOf[Consumer[AnyRef, AnyRef]])
    val tp1 = new TopicPartition("t1", 0)
    val duration = java.time.Duration.ofSeconds(10)
    Mockito.when(consumer.position(tp, duration)).thenReturn(0)
    Mockito.when(consumer.position(tp1, duration)).thenReturn(10)
    tracker.assignedPositionsAndSeek(Set(tp, tp1), consumer, duration)

    verifyOffsets(Map(tp -> 0, tp1 -> 10))
    Mockito.verify(consumer).position(tp, duration)
    Mockito.verify(consumer).position(tp1, duration)

    // revoke
    tracker.revoke(Set(tp1))
    verifyOffsets(Map(tp -> 0))
    tracker.revoke(Set(tp))
    verifyOffsets(Map())
  }

  private def offset(off: Long) = new OffsetAndMetadata(off)
}
