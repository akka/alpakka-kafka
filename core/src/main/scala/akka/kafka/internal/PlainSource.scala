/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util
import java.util.regex.Pattern

import akka.NotUsed
import akka.kafka.Subscriptions._
import akka.kafka.{ConsumerSettings, Subscription}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, ConsumerRecord, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
 * @author carsten
 */
private[kafka] object PlainSource {
  def apply[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[ConsumerRecord[K, V], NotUsed] = {
    Source.fromGraph(new PlainSource(settings, subscription))
  }
}

private[kafka] class PlainSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription) extends GraphStage[SourceShape[ConsumerRecord[K, V]]] {
  private case object POLL
  private var consumer: Consumer[K, V] = _
  private val outlet = Outlet[ConsumerRecord[K, V]]("metalog-source-outlet")
  private val POLL_TIMEOUT = settings.pollTimeout
  private val POLL_INTERVAL = settings.pollInterval

  private var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  override def shape: SourceShape[ConsumerRecord[K, V]] = SourceShape(outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
    setHandler(outlet, new OutHandler {
      override def onPull(): Unit = {
        tryPush()
      }
    })

    private def tryPush(): Unit = {
      if (isAvailable(outlet) && buffer.hasNext) {
        val rec = buffer.next
        push(outlet, rec)
      }
      else if (buffer.isEmpty) {
        pollKafka()
      }
    }

    private def pollKafka(): Unit = {
      if (buffer.isEmpty) {
        val records = consumer.poll(POLL_TIMEOUT.toMillis)
        if (!records.isEmpty) {
          buffer = records.iterator().asScala
          tryPush()
        }
        else {
          scheduleOnce(POLL, POLL_INTERVAL)
        }
      }
    }

    override protected def onTimer(timerKey: Any): Unit = {
      pollKafka()
      tryPush()
    }

    override def postStop(): Unit = {
      consumer.close()
    }

    override def preStart(): Unit = {
      consumer = settings.createKafkaConsumer()
      subscription match {
        case TopicSubscription(topics) => consumer.subscribe(topics.asJava)
        case TopicSubscriptionPattern(topics) =>
          consumer.subscribe(Pattern.compile(topics), EmptyRebalanceListener)
        case Assignment(assignments) =>
          consumer.assign(assignments.asJava)
        case AssignmentWithOffset(assignments) =>
          consumer.assign(assignments.keySet.asJava)
          assignments foreach {
            case (topic, offset) => consumer.seek(topic, offset)
          }
        case AssignmentOffsetsForTimes(timestampsToSearch) =>
          val topicPartitionToOffsetAndTimestamp = consumer.offsetsForTimes(timestampsToSearch.mapValues(long2Long(_)).asJava)
          topicPartitionToOffsetAndTimestamp.asScala.foreach {
            case (tp, oat: OffsetAndTimestamp) =>
              val offset = oat.offset()
              val ts = oat.timestamp()
              consumer.seek(tp, offset)
          }
      }
    }

    private object EmptyRebalanceListener extends ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {}
    }

  }
}
