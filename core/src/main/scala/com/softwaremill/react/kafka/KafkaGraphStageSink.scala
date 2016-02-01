package com.softwaremill.react.kafka

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import kafka.producer.KafkaProducer

class KafkaGraphStageSink[T](
    val producer: KafkaProducer[T],
    props: ProducerProperties[T]
) extends GraphStage[SinkShape[T]] {

  val closeTimeoutMs = 1000L

  val in: Inlet[T] = Inlet("KafkaGraphStageSink")

  override val shape: SinkShape[T] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var closed = false

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val element = grab(in)
        try {
          if (!closed) producer.send(props.encoder.toBytes(element), props.partitionizer(element))
        }
        catch {
          case ex: Exception =>
            close()
            failStage(ex)
        }
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        close()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        close()
      }
    })

    def close(): Unit =
      if (!closed) {
        producer.close()
        closed = true
      }

    override def afterPostStop(): Unit = {
      close()
      super.afterPostStop()
    }

    override def preStart(): Unit = {
      pull(in)
    }
  }
}
