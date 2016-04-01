/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream._
import akka.stream.stage._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, RecordMetadata}

/**
 * INTERNAL API
 */
private[kafka] class ProducerStage[K, V, P](
  settings: ProducerSettings[K, V], producerProvider: () => KafkaProducer[K, V]
)
    extends GraphStage[FlowShape[Producer.Message[K, V, P], Future[Producer.Result[K, V, P]]]]
    with LazyLogging {

  private val in = Inlet[Producer.Message[K, V, P]]("messages")
  private val out = Outlet[Future[Producer.Result[K, V, P]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) = {
    val producer = producerProvider()
    val logic = new GraphStageLogic(shape) {
      var awaitingConfirmation = 0L
      var completionState: Option[Try[Unit]] = None

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
          val r = Promise[Producer.Result[K, V, P]]
          producer.send(msg.record, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
              if (exception == null)
                r.success(Producer.Result(metadata.offset, msg))
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
        logger.debug("Stage completed")
        try {
          producer.flush()
          producer.close(settings.closeTimeout.toMillis, TimeUnit.MILLISECONDS)
          logger.debug("Producer closed")
        }
        catch {
          case NonFatal(ex) => logger.error("Problem occurred during producer close", ex)
        }
        super.postStop()
      }
    }
    logic
  }
}
