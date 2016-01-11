package com.softwaremill.react.kafka

import java.util.concurrent.TimeUnit

import akka.stream.{Inlet, Attributes, SinkShape}
import akka.stream.stage.{InHandler, GraphStageLogic, GraphStage}
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.producer.ReactiveKafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaGraphStageSink[K, V](richProducer: ReactiveKafkaProducer[K, V])
    extends GraphStage[SinkShape[ProducerMessage[K, V]]] with LazyLogging {

  val producer = richProducer.producer
  val closeTimeoutMs = 1000L

  def close(): Unit = {
    richProducer.producer.close(closeTimeoutMs, TimeUnit.MILLISECONDS)
  }

  val in: Inlet[ProducerMessage[K, V]] = Inlet("KafkaGraphStageSink")

  override val shape: SinkShape[ProducerMessage[K, V]] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val element = grab(in)
        val record = richProducer.props.partitionizer(element.value) match {
          case Some(partitionId) => new ProducerRecord(richProducer.props.topic, partitionId, element.key, element.value)
          case None => new ProducerRecord(richProducer.props.topic, element.key, element.value)
        }
        try {
          producer.send(record)
        }
        catch {
          case ex: Exception =>
            close()
            failStage(ex)
        }
        logger.debug(s"Written item to Kafka: $record")
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        logger.debug(s"Stream finished, closing Kafka resources for topic ${richProducer.props.topic}")
        close()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        logger.error(s"Stream failed, closing Kafka resources for topic ${richProducer.props.topic}", ex)
        close()
      }
    })

    override def preStart(): Unit = {
      pull(in)
    }
  }
}
