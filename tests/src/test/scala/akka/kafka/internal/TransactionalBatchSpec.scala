/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerMessage.GroupTopicPartition
import akka.kafka.internal.TransactionalProducerStage.TransactionBatch.FirstAndHighestOffsets
import akka.kafka.internal.TransactionalSourceLogic.CommittedMarkerRef
import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka.{ConsumerMessage, ConsumerSettings}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{TestKit, TestProbe}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Promise
import scala.concurrent.duration._

class TransactionalBatchSpec(_system: ActorSystem)
    extends TestKit(_system)
    with WordSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with LogCapturing {

  def this() = this(ActorSystem())

  override def afterAll(): Unit =
    shutdown(system)

  val groupId = "group-id"
  val topic = "topic"
  val gtp = GroupTopicPartition(groupId, topic, 1)

  implicit val m = ActorMaterializer(ActorMaterializerSettings(_system).withFuzzing(true))
  implicit val ec = _system.dispatcher

  def consumerSettings(groupId: String): ConsumerSettings[String, String] =
    ConsumerSettings
      .create(system, new StringDeserializer, new StringDeserializer)
      .withGroupId(groupId)
      .withStopTimeout(10.millis)

  def committedMarker(sourceStageRef: ActorRef,
                      consumerSettings: ConsumerSettings[_, _],
                      onTxAborted: Promise[Unit],
                      onFirstMsgRcvd: Promise[Unit]): CommittedMarkerRef = {
    CommittedMarkerRef(sourceStageRef, consumerSettings.commitTimeout, onTxAborted, onFirstMsgRcvd)(ec)
  }

  /**
   * For interactions with batches see transactional tests in [[ProducerSpec]].
   */
  "Transactional batch" should {
    val offset = 1L
    val actor = TestProbe("TransactionSourceStage")
    val settings = consumerSettings(groupId)
    val marker = committedMarker(actor.ref, settings, null, null)
    val msgCommitMarker1 =
      ConsumerMessage.PartitionOffsetCommittedMarker(gtp, offset, marker, fromPartitionedSource = false)
    val msgCommitMarker2 =
      ConsumerMessage.PartitionOffsetCommittedMarker(gtp, offset + 1, marker, fromPartitionedSource = false)

    "create a non-empty batch when updating an empty batch" in {
      val nonEmptyBatch = TransactionalProducerStage.TransactionBatch.empty.updated(msgCommitMarker1)

      nonEmptyBatch.offsetMap() shouldBe Map(
        gtp.topicPartition -> new OffsetAndMetadata(offset + 1)
      )
    }

    "update a non-empty batch and report the latest offset" in {
      val nonEmptyBatch =
        TransactionalProducerStage.TransactionBatch.empty
          .updated(msgCommitMarker1)
          .updated(msgCommitMarker2)

      nonEmptyBatch.offsetMap() shouldBe Map(
        gtp.topicPartition -> new OffsetAndMetadata(offset + 2)
      )
    }

    "update a non-empty batch out of order and report the latest offset" in {
      val nonEmptyBatch =
        TransactionalProducerStage.TransactionBatch.empty
          .updated(msgCommitMarker2)
          .updated(msgCommitMarker1)

      nonEmptyBatch.offsetMap() shouldBe Map(
        gtp.topicPartition -> new OffsetAndMetadata(offset + 2)
      )
    }

    "first observed offset is first offset observed" in {
      val nonEmptyBatch =
        TransactionalProducerStage.TransactionBatch.empty
          .updated(msgCommitMarker1)
          .updated(msgCommitMarker2)

      nonEmptyBatch.offsets shouldBe Map(
        gtp -> FirstAndHighestOffsets(msgCommitMarker1.offset, msgCommitMarker2.offset)
      )
    }

    "first observed offset is first offset observed even when offsets are out of order" in {
      val nonEmptyBatch =
        TransactionalProducerStage.TransactionBatch.empty
          .updated(msgCommitMarker2)
          .updated(msgCommitMarker1)

      nonEmptyBatch.offsets shouldBe Map(
        gtp -> FirstAndHighestOffsets(msgCommitMarker2.offset, msgCommitMarker2.offset)
      )
    }
  }
}
