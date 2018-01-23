/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util
import java.util.regex.Pattern

import akka.{Done, NotUsed}
import akka.kafka.Subscriptions._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscription}
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, ConsumerRecord, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

/**
 * @author carsten
 */
private[kafka] object PlainSource {
  def apply[K, V](settings: ConsumerSettings[K, V], subscription: Subscription): Source[ConsumerRecord[K, V], Control] = {
    val consumer = settings.createKafkaConsumer()
    subscription match {
      case TopicSubscription(topics) => consumer.subscribe(topics.asJava)
      case TopicSubscriptionPattern(topics) =>
        consumer.subscribe(Pattern.compile(topics), new NoOpConsumerRebalanceListener)
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
    Source.fromGraph(new PlainSource(consumer))
  }

  def apply[K, V](consumer: Consumer[K, V]): Source[ConsumerRecord[K, V], Control] = {
    Source.fromGraph(new PlainSource(consumer))
  }
}

private[kafka] class PlainSource[K, V](consumer: Consumer[K, V]) extends GraphStageWithMaterializedValue[SourceShape[ConsumerRecord[K, V]], Control] {
  private case object POLL
  private val outlet = Outlet[ConsumerRecord[K, V]]("metalog-source-outlet")
  private val POLL_TIMEOUT = 50.millis
  private val POLL_INTERVAL = 50.millis

  private var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  override def shape: SourceShape[ConsumerRecord[K, V]] = SourceShape(outlet)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = createLogic()
    (logic, logic)
  }

  private def createLogic() = new TimerGraphStageLogic(shape) with Control {
    private val controlPromise = Promise[Done]

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
      controlPromise.success(Done)
    }

    override def shutdown(): Future[Done] = {
      callback.invoke(())
      controlPromise.future
    }

    private val callback = getAsyncCallback[Unit] {
      _ =>
        completeStage()
    }

    override def stop(): Future[Done] = shutdown()

    override def isShutdown: Future[Done] = {
      controlPromise.future
    }

  }
}
