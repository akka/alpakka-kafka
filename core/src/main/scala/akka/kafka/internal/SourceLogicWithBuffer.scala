/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import akka.annotation.InternalApi
import akka.stream.Outlet
import akka.stream.stage.{AsyncCallback, GraphStageLogic, OutHandler}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

@InternalApi
private trait SourceLogicWithBuffer[K, V, Msg] {
  self: GraphStageLogic with StageIdLogging with PromiseControl =>

  def out: Outlet[Msg]

  // TODO: can pump impl be factored out?
  protected def pump(): Unit

  protected var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  protected val filterRevokedPartitionsCB: AsyncCallback[Set[TopicPartition]] = getAsyncCallback[Set[TopicPartition]] {
    tps =>
      filterRevokedPartitions(tps)
  }

  protected def filterRevokedPartitions(topicPartitions: Set[TopicPartition]): Unit = {
    if (topicPartitions.nonEmpty) {
      log.debug("filtering out messages from revoked partitions {}", topicPartitions)
      // as buffer is an Iterator the filtering will be applied during `pump`
      buffer = buffer.filterNot { record =>
        log.debug(s"Filtering msg: $record")
        val tp = new TopicPartition(record.topic, record.partition)
        topicPartitions.contains(tp)
      }
      log.debug("filtering applied")
    }
    resumeDemand()
  }

  protected def suspendDemand(): Unit = {
    log.debug("Suspend demand")
    setHandler(out, new OutHandler {
      override def onPull(): Unit = ()
      override def onDownstreamFinish(): Unit =
        performShutdown()
    })
  }

  protected def resumeDemand(): Unit = {
    log.debug("Resume demand")
    setHandler(out, new OutHandler {
      override def onPull(): Unit = pump()
      override def onDownstreamFinish(): Unit =
        performShutdown()
    })
  }
}
