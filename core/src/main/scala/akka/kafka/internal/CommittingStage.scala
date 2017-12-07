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
import akka.stream.stage._
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Source}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * @author carsten
 */
final case class BidiSourceShape[-FO, +FI, -O](flowOut: Outlet[FO @uncheckedVariance], flowIn: Inlet[FI @uncheckedVariance], out: Outlet[O @uncheckedVariance]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = flowIn :: Nil
  override val outlets: immutable.Seq[Outlet[_]] = flowOut :: out :: Nil

  override def deepCopy(): BidiSourceShape[FO, FI, O] = BidiSourceShape(flowOut.carbonCopy(), flowIn.carbonCopy(), out.carbonCopy())
}

object CommittingStage {
  def apply[K, V](settings: ConsumerSettings[K, V], subscription: Subscription, flow: Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], NotUsed], commitBatch: Int = 1): Source[ConsumerRecord[K, V], NotUsed] = {
    Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val stage = b.add(new CommittingStage(settings, subscription, commitBatch))
      val handlerFlow = b.add(flow)

      stage.flowOut ~> handlerFlow ~> stage.flowIn

      SourceShape(stage.out)
    })
  }
}

private class CommittingStage[K, V](settings: ConsumerSettings[K, V], subscription: Subscription, commitBatch: Int) extends GraphStage[BidiSourceShape[ConsumerRecord[K, V], ConsumerRecord[K, V], ConsumerRecord[K, V]]] {
  private case object POLL
  private case object COMMIT
  private var consumer: Consumer[K, V] = _
  private val flowOut = Outlet[ConsumerRecord[K, V]]("committing-stage-flow-out")
  private val flowIn = Inlet[ConsumerRecord[K, V]]("committing-stage-flow-in")
  private val out = Outlet[ConsumerRecord[K, V]]("committing-stage-out")
  // FIXME: configuration
  private val POLL_TIMEOUT = 50.millis
  private val POLL_INTERVAL = 50.millis
  private val COMMIT_IDLE = 10000.millis

  private var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  private var recordsSinceLastCommit = 0
  private var commitBuffer = Map.empty[TopicPartition, OffsetAndMetadata]
  private var commitScheduled: Boolean = false

  override def shape: BidiSourceShape[ConsumerRecord[K, V], ConsumerRecord[K, V], ConsumerRecord[K, V]] = BidiSourceShape(flowOut, flowIn, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        tryPull(flowIn)
      }

      override def onDownstreamFinish(): Unit = {
        tryCommit(force = true)
        super.onDownstreamFinish()
      }
    })

    setHandler(flowOut, new OutHandler {
      override def onPull(): Unit = {
        tryPush()
      }
    })

    setHandler(flowIn, new InHandler {
      override def onPush(): Unit = {
        val rec = grab(flowIn)
        val tp = new TopicPartition(rec.topic(), rec.partition())
        val om = new OffsetAndMetadata(rec.offset() + 1)
        commitBuffer += tp -> om
        recordsSinceLastCommit += 1
        tryCommit()
        push(out, rec)
      }
    })

    private def tryCommit(force: Boolean = false): Unit = {
      if (force || recordsSinceLastCommit == commitBatch) {
        if (commitBuffer.nonEmpty) {
          consumer.commitSync(commitBuffer.asJava)
          commitBuffer = Map.empty[TopicPartition, OffsetAndMetadata]
          recordsSinceLastCommit = 0
        }
      }
      else {
        if (!commitScheduled) {
          scheduleOnce(COMMIT, COMMIT_IDLE)
          commitScheduled = true
        }
      }
    }

    private def tryPush(): Unit = {
      if (isAvailable(flowOut) && buffer.hasNext) {
        val rec = buffer.next
        push(flowOut, rec)
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
      timerKey match {
        case COMMIT =>
          commitScheduled = false
          tryCommit(force = true)
        case POLL =>
          pollKafka()
          tryPush()
      }
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

    override def postStop(): Unit = {
      consumer.close()
    }
  }

}
