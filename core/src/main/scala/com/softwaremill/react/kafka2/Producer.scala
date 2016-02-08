package com.softwaremill.react.kafka2

import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.{Sink, Flow}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}


object Producer {
  def value2record[V](topic: String) = Flow[V].map(new ProducerRecord[Array[Byte], V](topic, _))

  def apply[K, V](producerProvider: () => KafkaProducer[K, V]) = {
    Flow.fromGraph(new ProducerSendFlowStage(producerProvider))
  }
  def sink[K, V](producerProvider: () => KafkaProducer[K, V]) = {
    apply(producerProvider).to(Sink.ignore)
  }
}

class ProducerSendFlowStage[K, V](producerProvider: () => KafkaProducer[K, V])
  extends GraphStageWithMaterializedValue[FlowShape[ProducerRecord[K, V], Future[(ProducerRecord[K, V], RecordMetadata)]], KafkaProducer[K, V]]
  with LazyLogging
{
  private val messages = Inlet[ProducerRecord[K, V]]("messages")
  private val confirmation = Outlet[Future[(ProducerRecord[K, V], RecordMetadata)]]("confirmation")
  val shape = new FlowShape(messages, confirmation)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val closeTimeout = 60000L
    val producer = producerProvider()
    val logic = new GraphStageLogic(shape) {
      setHandler(confirmation, new OutHandler {
        override def onPull(): Unit = {
          tryPull(messages)
        }
      })

      setHandler(messages, new InHandler {
        override def onPush(): Unit = {
          val msg = grab(messages)
          val result = Promise[(ProducerRecord[K, V], RecordMetadata)]
          producer.send(msg, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              val completion = Option(metadata).map(m => Success((msg, m))).getOrElse(Failure(exception))
              result.complete(completion)
              ()
            }
          })
          push(confirmation, result.future)
        }
      })

      override def postStop(): Unit = {
        logger.debug("Stage completed")
        try {
          producer.flush()
          producer.close(closeTimeout, TimeUnit.MILLISECONDS)
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