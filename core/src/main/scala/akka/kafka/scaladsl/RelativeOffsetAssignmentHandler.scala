/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.RestrictedConsumer
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._

object RelativeOffsetAssignmentHandler {
  def apply(backOffset: Long): RelativeOffsetAssignmentHandler =
    new RelativeOffsetAssignmentHandler(None, Some(backOffset))

  def apply(topicPartition: TopicPartition, backOffset: Long): RelativeOffsetAssignmentHandler =
    new RelativeOffsetAssignmentHandler(Some(Map(topicPartition -> backOffset)))

  def apply(topicPartitionBackOffset: Map[TopicPartition, Long]): RelativeOffsetAssignmentHandler =
    new RelativeOffsetAssignmentHandler(Some(topicPartitionBackOffset))

  def apply(tps: Set[TopicPartition], backOffset: Long): RelativeOffsetAssignmentHandler = {
    val partitionsOffset = tps.map(tp => (tp, backOffset)).toMap
    new RelativeOffsetAssignmentHandler(Some(partitionsOffset))
  }
}

class RelativeOffsetAssignmentHandler private (topicPartitionsBackOffset: Option[Map[TopicPartition, Long]],
                                               backOffset: Option[Long] = None)
    extends PartitionAssignmentHandler {

  override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

  override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
    topicPartitionsBackOffset match {
      case Some(partitionOffset) =>
        val tps = partitionOffset.keys.filter(assignedTps.contains)
        consumer.endOffsets(tps.toSet.asJava).asScala.foreach { tpOffset =>
          val partition = tpOffset._1
          val endOffset = tpOffset._2
          val offset = endOffset - partitionOffset.getOrElse(partition, 0L)
          consumer.seek(partition, offset)
        }
      case None =>
        backOffset.foreach { offset =>
          assignedTps.foreach { tp =>
            val endOffset = consumer.endOffsets(assignedTps.asJava).get(tp)
            val expectedOffset = endOffset - offset
            consumer.seek(tp, expectedOffset)
          }
        }
    }
  }

  override def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

  override def onStop(currentTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()
}
