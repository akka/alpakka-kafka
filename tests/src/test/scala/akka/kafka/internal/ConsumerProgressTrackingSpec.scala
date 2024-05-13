/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.kafka.tests.scaladsl.LogCapturing
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

class ConsumerProgressTrackingSpec extends AnyFlatSpecLike with Matchers with LogCapturing {

  private val tp = new TopicPartition("t", 0)
  private val m1 = new ConsumerRecord[String, String](tp.topic(), tp.partition(), 10L, "k1", "kv")
  def asConsumerRecords[K, V](tp: TopicPartition, records: ConsumerRecord[K, V]*): ConsumerRecords[K, V] = {
    new ConsumerRecords[K, V](Map(tp -> records.asJava).asJava)
  }
  private val records = asConsumerRecords(tp, m1)

  private def extractOffsetFromSafe(entry: (TopicPartition, SafeOffsetAndTimestamp)) = (entry._1, entry._2.offset)
  private def extractOffset(entry: (TopicPartition, OffsetAndMetadata)) = (entry._1, entry._2.offset())

  it should "add requested offsets" in {
    val tracker = new ConsumerProgressTrackerImpl()
    tracker.commitRequested shouldBe empty
    tracker.committedOffsets shouldBe empty
    tracker.commitRequested(Map(tp -> offset(0)))
    tracker.commitRequested should have size 1
    tracker.commitRequested(tp).offset() should be(0L)
    tracker.receivedMessages shouldBe empty
    tracker.committedOffsets shouldBe empty
  }

  it should "track received records" in {
    val tracker = new ConsumerProgressTrackerImpl()
    tracker.assignedPositions(Set(tp), Map(tp -> 0L))
    tracker.received(records)

    tracker.commitRequested(tp).offset() should be(0L)
    tracker.receivedMessages should have size 1
    tracker.receivedMessages(tp).offset should be(10L)
    tracker.receivedMessages(tp).timestamp should be(ConsumerRecord.NO_TIMESTAMP)
  }

  it should "filter out non-assigned partitions in received messages" in {
    val tracker = new ConsumerProgressTrackerImpl()
    // received, but wasn't requested (assigned), so empty received/requested/committed
    tracker.received(asConsumerRecords(tp, m1))
    tracker.commitRequested shouldBe empty
    tracker.receivedMessages shouldBe empty
    tracker.committedOffsets shouldBe empty

    // once is is assigned, we should start tracking it
    tracker.assignedPositions(Set(tp), Map(tp -> 0L))
    // haven't received any records yet, stays empty
    tracker.receivedMessages shouldBe empty
    tracker.commitRequested.map(extractOffset) should be(Map(tp -> 0L))
    tracker.committedOffsets.map(extractOffset) should be(Map(tp -> 0L))

    // gets a message, but not commit progress
    tracker.received(records)
    tracker.receivedMessages.map(extractOffsetFromSafe) should be(Map(tp -> 10L))
    tracker.commitRequested.map(extractOffset) should be(Map(tp -> 0L))
    tracker.committedOffsets.map(extractOffset) should be(Map(tp -> 0L))

    // receive a message that is not assigned, that is ignored
    val tp2 = new TopicPartition("t", 2)
    tracker.received(
      new ConsumerRecords[String, String](
        Map(
          tp2 -> List(new ConsumerRecord[String, String](tp2.topic(), tp2.partition(), 10L, "k1", "kv")).asJava
        ).asJava
      )
    )
    tracker.receivedMessages.map(extractOffsetFromSafe) should be(Map(tp -> 10L))
    // no change to the committing
    tracker.commitRequested.map(extractOffset) should be(Map(tp -> 0L))
    tracker.committedOffsets.map(extractOffset) should be(Map(tp -> 0L))
  }

  it should "track offsets when they are committed" in {
    val tracker = new ConsumerProgressTrackerImpl()
    tracker.commitRequested shouldBe empty
    tracker.committedOffsets shouldBe empty
    val tp = new TopicPartition("t", 0)
    tracker.commitRequested(Map(tp -> offset(0L)))
    tracker.committed(Map(tp -> offset(1L)).asJava)
    // requested shouldn't change, just the committed
    tracker.commitRequested(tp).offset() should be(0L)
    tracker.committedOffsets(tp).offset() should be(1L)
  }

  /**
   * This is expected behavior and safety is expected to be handled by caller. This test just ensures the behavior
   * matches the docs; that is, we can request/commit to partitions that have not been assigned. This was done to
   * keep the commit path uncomplicated and low latency.
   */
  it should "allow request/commit for non-tracked partitions" in {
    val tracker = new ConsumerProgressTrackerImpl()
    // request a commit
    tracker.commitRequested(Map(tp -> new OffsetAndMetadata(10L)))
    tracker.receivedMessages shouldBe empty
    tracker.commitRequested.map(extractOffset) should be(Map(tp -> 10L))
    tracker.committedOffsets shouldBe empty

    // commit completed, but still not actually 'assigned' that partition
    tracker.committed(Map(tp -> new OffsetAndMetadata(10L)).asJava)
    tracker.receivedMessages shouldBe empty
    tracker.commitRequested.map(extractOffset) should be(Map(tp -> 10L))
    tracker.committedOffsets.map(extractOffset) should be(Map(tp -> 10L))
  }

  it should "not overwrite existing committed offsets when assigning" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val tp = new TopicPartition("t", 0)
    tracker.committed(Map(tp -> offset(1L)).asJava)
    tracker.assignedPositions(Set(tp), Map(tp -> 2L))
    tracker.committedOffsets(tp).offset() should be(1L)
  }

  it should "revoke assigned offsets" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val tp1 = new TopicPartition("t", 1)
    tracker.commitRequested(Map(tp -> offset(0L), tp1 -> offset(1L)))
    tracker.received(records)
    tracker.committed(Map(tp -> offset(1L)).asJava)
    tracker.revoke(Set(tp))
    tracker.commitRequested should have size 1
    tracker.commitRequested(tp1).offset() should be(1L)
    tracker.receivedMessages shouldBe empty
    tracker.committedOffsets shouldBe empty
  }

  it should "directly use assigned positions" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val tp0 = new TopicPartition("t", 0)
    val tp1 = new TopicPartition("t", 1)
    tracker.assignedPositions(Set(tp0, tp1), Map(tp0 -> 0L, tp1 -> 1L))
    tracker.commitRequested(tp0).offset() should be(0L)
    tracker.commitRequested(tp1).offset() should be(1L)
    tracker.committedOffsets(tp0).offset() should be(0L)
    tracker.committedOffsets(tp1).offset() should be(1L)
  }

  it should "lookup positions from the consumer when assigned" in {
    val tracker = new ConsumerProgressTrackerImpl()
    val consumer = Mockito.mock(classOf[Consumer[AnyRef, AnyRef]])
    val tp0 = new TopicPartition("t", 0)
    val duration = java.time.Duration.ofSeconds(10)
    Mockito.when(consumer.position(tp0, duration)).thenReturn(5L)
    tracker.assignedPositionsAndSeek(Set(tp0), consumer, duration)
    tracker.commitRequested(tp0).offset() should be(5L)
    tracker.committedOffsets(tp0).offset() should be(5L)
  }

  it should "pass through requests to listeners" in {
    val tracker = new ConsumerProgressTrackerImpl()
    class Listener extends ConsumerAssignmentTrackingListener {
      var state = Map[TopicPartition, Long]()
      override def revoke(revokedTps: Set[TopicPartition]): Unit = {
        state = state.filter { case (tp, _) => !revokedTps.contains(tp) }
      }
      override def assignedPositions(assignedTps: Set[TopicPartition],
                                     assignedOffsets: Map[TopicPartition, Long]): Unit = {
        state = state ++ assignedOffsets
      }
    }
    val listener = new Listener()
    tracker.addProgressTrackingCallback(listener)

    def verifyOffsets(offsets: Map[TopicPartition, Long]): Unit = {
      listener.state should be(offsets)
    }

    // assign
    val consumer = Mockito.mock(classOf[Consumer[AnyRef, AnyRef]])
    val tp1 = new TopicPartition("t1", 0)
    val duration = java.time.Duration.ofSeconds(10)
    Mockito.when(consumer.position(tp, duration)).thenReturn(0L)
    Mockito.when(consumer.position(tp1, duration)).thenReturn(10L)
    tracker.assignedPositionsAndSeek(Set(tp, tp1), consumer, duration)

    verifyOffsets(Map(tp -> 0L, tp1 -> 10L))
    Mockito.verify(consumer).position(tp, duration)
    Mockito.verify(consumer).position(tp1, duration)

    // revoke
    tracker.revoke(Set(tp1))
    verifyOffsets(Map(tp -> 0L))
    tracker.revoke(Set(tp))
    verifyOffsets(Map())
  }

  private def offset(off: Long) = new OffsetAndMetadata(off)
}
