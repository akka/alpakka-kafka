package com.softwaremill.react.kafka

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.{Failure, Success, Try}

class KafkaGraphStageSource[K, V](consumerAndProps: ReactiveKafkaConsumer[K, V])
    extends GraphStage[SourceShape[ConsumerRecord[K, V]]] with LazyLogging {

  val out: Outlet[ConsumerRecord[K, V]] = Outlet("KafkaGraphStageSource")
  val pollTimeoutMs = consumerAndProps.properties.pollTimeout.toMillis
  val pollRetryDelayMs = consumerAndProps.properties.pollRetryDelay
  val consumer = consumerAndProps.consumer

  override val shape: SourceShape[ConsumerRecord[K, V]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =

    new TimerGraphStageLogic(shape) {
      var buffer: Option[java.util.Iterator[ConsumerRecord[K, V]]] = None

      override protected def onTimer(timerKey: Any): Unit = {
        readSingleElement()
      }

      private def pollIterator() = {
        buffer match {
          case Some(iterator) =>
            iterator
          case None =>
            logger.debug("Polling consumer")
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
            else scheduleOnce("timer", pollRetryDelayMs)
          case Failure(ex) =>
            consumer.close()
            fail(out, ex)
        }
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          readSingleElement()

        }

        override def onDownstreamFinish(): Unit = {
          logger.debug("Closing Kafka reader due to onDownstreamFinish")
          consumer.close()
          super.onDownstreamFinish()
        }
      })
    }
}
