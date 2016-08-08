/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka

import java.util.{Map => JMap}
import java.util.concurrent.CompletionStage

import akka.Done
import akka.kafka.internal.ConsumerStage.CommittableOffsetBatchImpl
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

/**
 * Classes that are used in both [[javadsl.Consumer]] and
 * [[scaladsl.Consumer]].
 */
object ConsumerMessage {
  /**
   * Output element of `committableSource`.
   * The offset can be committed via the included [[CommittableOffset]].
   */
  final case class CommittableMessage[K, V](
      record: ConsumerRecord[K, V],
      committableOffset: CommittableOffset
  )

  /**
   * Commit an offset that is included in a [[CommittableMessage]].
   * If you need to store offsets in anything other than Kafka, this API
   * should not be used.
   *
   * This interface might move into `akka.stream`
   */
  trait Committable {
    def commitScaladsl(): Future[Done]
    def commitJavadsl(): CompletionStage[Done]
  }

  /**
   * Included in [[CommittableMessage]]. Makes it possible to
   * commit an offset with the [[Committable#commitScaladsl]] / [[Committable#commitJavadsl]] method
   * or aggregate several offsets in a [[CommittableOffsetBatch batch]]
   * before committing.
   *
   * Note that the offset position that is committed to Kafka will automatically
   * be one more than the `offset` of the message, because the committed offset
   * should be the next message your application will consume,
   * i.e. lastProcessedMessageOffset + 1.
   */
  trait CommittableOffset extends Committable {
    /**
     * Information about the offset position for a
     * clientId, topic, partition.
     */
    def partitionOffset: PartitionOffset
  }

  /**
   * Offset position for a clientId, topic, partition.
   */
  final case class PartitionOffset(key: ClientTopicPartition, offset: Long)

  /**
   * clientId, topic, partition key for an offset position.
   */
  final case class ClientTopicPartition(
    clientId: String,
    topic: String,
    partition: Int
  )

  object CommittableOffsetBatch {
    val empty: CommittableOffsetBatch = new CommittableOffsetBatchImpl(Map.empty, Map.empty)
  }

  val emptyCommittableOffsetBatch: CommittableOffsetBatch = CommittableOffsetBatch.empty

  /**
   * For improved efficiency it is good to aggregate several [[CommittableOffset]],
   * using this class, before [[Committable#commitScaladsl committing]] them. Start with
   * the [[CommittableOffsetBatch$#empty empty] batch.
   */
  trait CommittableOffsetBatch extends Committable {
    /**
     * Add/overwrite an offset position for the given clientId, topic, partition.
     */
    def updated(offset: CommittableOffset): CommittableOffsetBatch

    /**
     * Scala API: Get current offset positions
     */
    def offsets(): Map[ClientTopicPartition, Long]

    /**
     * Java API: Get current offset positions
     */
    def getOffsets(): JMap[ClientTopicPartition, Long]
  }

}
