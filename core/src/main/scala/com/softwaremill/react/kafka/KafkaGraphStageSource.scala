package com.softwaremill.react.kafka

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.softwaremill.react.kafka.commit.OffsetMap
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class KafkaGraphStageSource[K, V](
  consumerAndProps: ReactiveKafkaConsumer[K, V],
  partitionOffsetMap: OffsetMap = OffsetMap()
)
    extends GraphStage[SourceShape[ConsumerRecord[K, V]]] with LazyLogging {

  val out: Outlet[ConsumerRecord[K, V]] = Outlet("KafkaGraphStageSource")
  val pollTimeoutMs = consumerAndProps.properties.pollTimeout.toMillis
  val pollRetryDelayMs = consumerAndProps.properties.pollRetryDelay
  val consumer = consumerAndProps.consumer
  val TimerPollKey = "timer-poll"
  val CommitPollKey = "timer-commit"
  val DefaultCommitInterval = 10 seconds
  var committedOffsetMap = OffsetMap()

  override val shape: SourceShape[ConsumerRecord[K, V]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =

    new TimerGraphStageLogic(shape) {
      var buffer: Option[java.util.Iterator[ConsumerRecord[K, V]]] = None
      var closed = false

      override def postStop(): Unit = {
        close()
        super.postStop()
      }

      override protected def onTimer(timerKey: Any): Unit = {
        if (timerKey == TimerPollKey)
          readSingleElement()
        else if (timerKey == CommitPollKey && !closed)
          performManualCommit()
      }

      override def preStart(): Unit = {
        if (consumerAndProps.properties.hasManualCommit)
          scheduleManualCommit()
        super.beforePreStart()
      }

      private def pollIterator(): Iterator[ConsumerRecord[K, V]] = {
        buffer match {
          case Some(iterator) =>
            iterator
          case None if !closed =>
            consumer.poll(pollTimeoutMs).iterator()
        }
      }

      private def readSingleElement(): Unit = {
        Try(pollIterator()) match {
          case Success(iterator) =>
            if (iterator.hasNext) {
              val record = iterator.next()
              push(out, record)
              if (iterator.hasNext) {
                // there's still some data left in the iterator
                buffer = Some(iterator)
              }
              else buffer = None
            }
            else scheduleOnce(TimerPollKey, pollRetryDelayMs)
          case Failure(ex) => fail(out, ex)
        }
      }

      def performManualCommit(): Unit = {
        val partitionOffsetMapCopy = partitionOffsetMap.copy()
        logger.debug(s"Flushing offsets to commit. Registered offsets: $partitionOffsetMapCopy vs $committedOffsetMap")
        val offsetMapToFlush = partitionOffsetMapCopy.diff(committedOffsetMap)
        if (offsetMapToFlush.nonEmpty) {
          try {
            consumer.commitSync(offsetMapToFlush.toCommitRequestInfo)
            committedOffsetMap = partitionOffsetMapCopy
            logger.debug(s"Committed offsets: $offsetMapToFlush")
          }
          catch {
            case ex: Exception =>
              logger.error(s"Manual commit failed for offsets: $offsetMapToFlush", ex)
              failStage(ex)
          }
        }
      }

      def close(): Unit =
        if (!closed) {
          closed = true
          consumer.close()
        }

      def scheduleManualCommit(): Unit = {
        logger.debug(s"Scheduling manual commit (topic ${consumerAndProps.properties.topic})")
        schedulePeriodically(CommitPollKey, consumerAndProps.properties.commitInterval.getOrElse(DefaultCommitInterval))
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          readSingleElement()
        }

        override def onDownstreamFinish(): Unit = {
          logger.debug(s"Closing Kafka reader due to onDownstreamFinish (topic ${consumerAndProps.properties.topic})")
          close()
          super.onDownstreamFinish()
        }
      })
    }
}
