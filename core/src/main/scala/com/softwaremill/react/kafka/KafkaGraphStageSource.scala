package com.softwaremill.react.kafka

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class KafkaGraphStageSource[T](consumerAndProps: KafkaConsumer[T])
    extends GraphStage[SourceShape[KafkaMessage[T]]] {

  val out: Outlet[KafkaMessage[T]] = Outlet("KafkaGraphStageSource")
  val pollTimeoutMs = consumerAndProps.props.consumerTimeoutMs
  val pollRetryDelay = 100 millis // TODO config
  val consumer = consumerAndProps.connector
  val iterator = consumerAndProps.iterator()
  val TimerPollKey = "timer-poll"

  override val shape: SourceShape[KafkaMessage[T]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      var closed = false

      override def afterPostStop(): Unit = {
        close()
        super.afterPostStop()
      }

      def close(): Unit =
        if (!closed) {
          closed = true
          consumer.shutdown()
        }

      private def tryReadingSingleElement(): Try[Option[KafkaMessage[T]]] = {
        Try {
          if (iterator.hasNext()) Option(iterator.next()) else None
        } recover {
          // We handle timeout exceptions as normal 'end of the queue' cases
          case _: ConsumerTimeoutException => None
        }
      }

      private def readSingleElement(): Unit = {
        tryReadingSingleElement() match {
          case Success(None) =>
            scheduleOnce(TimerPollKey, pollRetryDelay)
          case Success(Some(element)) =>
            push(out, element)
          case Failure(ex) =>
            close()
            fail(out, ex)
        }
      }

      override protected def onTimer(timerKey: Any): Unit = {
        if (timerKey == TimerPollKey && !closed)
          readSingleElement()
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          readSingleElement()
        }

        override def onDownstreamFinish(): Unit = {
          close()
          super.onDownstreamFinish()
        }
      })

    }

}
