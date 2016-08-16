/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.concurrent.TimeUnit

import akka.kafka.ProducerMessage.{Message, Result}
import akka.kafka.ProducerSettings
import akka.stream._
import akka.stream.stage._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, RecordMetadata}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
private[kafka] class ProducerStage[K, V, P](
  closeTimeout: FiniteDuration, producerProvider: () => KafkaProducer[K, V]
)
    extends GraphStage[FlowShape[Message[K, V, P], Future[Result[K, V, P]]]] {

  private val in = Inlet[Message[K, V, P]]("messages")
  private val out = Outlet[Future[Result[K, V, P]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) = {
    val producer = producerProvider()
    val logic = new GraphStageLogic(shape) with StageLogging {
      var awaitingConfirmation = 0L
      var completionState: Option[Try[Unit]] = None

      override protected def logSource: Class[_] = classOf[ProducerStage[K, V, P]]

      def checkForCompletion() = {
        if (isClosed(in) && awaitingConfirmation == 0) {
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }
      }

      val decrementConfirmation = getAsyncCallback[Unit] { _ =>
        awaitingConfirmation -= 1
        checkForCompletion()
        ()
      }

      setHandler(out, new OutHandler {
        override def onPull() = {
          tryPull(in)
        }
      })

      setHandler(in, new InHandler {
        override def onPush() = {
          val msg = grab(in)
          val r = Promise[Result[K, V, P]]
          producer.send(msg.record, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
              if (exception == null)
                r.success(Result(metadata.offset, msg))
              else
                r.failure(exception)
              decrementConfirmation.invoke(())
            }
          })
          awaitingConfirmation += 1
          push(out, r.future)
        }

        override def onUpstreamFinish() = {
          completionState = Some(Success(()))
          checkForCompletion()
        }

        override def onUpstreamFailure(ex: Throwable) = {
          completionState = Some(Failure(ex))
          checkForCompletion()
        }
      })

      override def postStop() = {
        log.debug("Stage completed")
        try {
          producer.flush()
          producer.close(closeTimeout.toMillis, TimeUnit.MILLISECONDS)
          log.debug("Producer closed")
        }
        catch {
          case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
        }
        super.postStop()
      }
    }
    logic
  }
}
