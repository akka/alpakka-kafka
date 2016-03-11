package com.softwaremill.react.kafka2

import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Producer {
  def value2record[V](topic: String) = Flow[V].map(new ProducerRecord[Array[Byte], V](topic, _))

  // FIXME is the KafkaProducer materialized value needed?
  def apply[K, V](producerProvider: () => KafkaProducer[K, V]): Flow[ProducerRecord[K, V], Future[(ProducerRecord[K, V], RecordMetadata)], KafkaProducer[K, V]] = {
    Flow.fromGraph(new ProducerSendFlowStage(producerProvider))
  }
  def sink[K, V](producerProvider: () => KafkaProducer[K, V]) = {
    apply(producerProvider).to(Sink.ignore)
  }
}

class ProducerSendFlowStage[K, V](producerProvider: () => KafkaProducer[K, V])
    extends GraphStageWithMaterializedValue[FlowShape[ProducerRecord[K, V], Future[(ProducerRecord[K, V], RecordMetadata)]], KafkaProducer[K, V]]
    with LazyLogging {
  private val messages = Inlet[ProducerRecord[K, V]]("messages")
  private val confirmation = Outlet[Future[(ProducerRecord[K, V], RecordMetadata)]]("confirmation")
  val shape = new FlowShape(messages, confirmation)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val closeTimeout = 60000L
    val producer = producerProvider()
    val logic = new GraphStageLogic(shape) {
      var awaitingConfirmation = 0L
      var completionState: Option[Try[Unit]] = None

      def checkForCompletion() = {
        if (isClosed(messages) && awaitingConfirmation == 0) {
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

      setHandler(confirmation, new OutHandler {
        override def onPull() = {
          tryPull(messages)
        }
      })

      setHandler(messages, new InHandler {
        override def onPush() = {
          val msg = grab(messages)
          val result = Promise[(ProducerRecord[K, V], RecordMetadata)]
          producer.send(msg, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
              val completion = Option(metadata).map(m => Success((msg, m))).getOrElse(Failure(exception))
              result.complete(completion)
              decrementConfirmation.invoke(())
              ()
            }
          })
          awaitingConfirmation += 1
          push(confirmation, result.future)
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
          producer.close(closeTimeout, TimeUnit.MILLISECONDS)
          logger.debug("Producer closed")
        }
        catch {
          case NonFatal(ex) => logger.error("Problem occurred during producer close", ex)
        }
        super.postStop()
      }
    }
    (logic, producer)
  }
}
