package com.softwaremill.react.kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorLogging
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy}
import kafka.producer.ReactiveKafkaProducer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, ProducerRecord}

sealed trait ProducerMessage[K, V] {
  def key: K
  def value: V
}

case class KeyValueProducerMessage[K, V](key: K, value: V) extends ProducerMessage[K, V]

case class ValueProducerMessage[V](value: V) extends ProducerMessage[Array[Byte], V] {
  override def key: Array[Byte] = null
}

object ProducerMessage {
  def apply[K, V](consumerRecord: ConsumerRecord[K, V]): ProducerMessage[K, V] =
    new KeyValueProducerMessage(consumerRecord.key(), consumerRecord.value())

  def apply[K, V](k: K, v: V): ProducerMessage[K, V] =
    new KeyValueProducerMessage(k, v)

  def apply[V](v: V): ProducerMessage[Array[Byte], V] = {
    new ValueProducerMessage(v)
  }
}

private[kafka] class KafkaActorSubscriber[K, V](
  val richProducer: ReactiveKafkaProducer[K, V],
  requestStrategyProvider: () => RequestStrategy
)
    extends ActorSubscriber with ActorLogging {

  override protected val requestStrategy = requestStrategyProvider()
  val closeTimeoutMs = 1000L

  def receive = {
    case ActorSubscriberMessage.OnNext(element) =>
      processElement(element.asInstanceOf[ProducerMessage[K, V]])
    case ActorSubscriberMessage.OnError(ex) =>
      handleError(ex)
    case ActorSubscriberMessage.OnComplete =>
      stop()
  }

  private def processElement(element: ProducerMessage[K, V]) = {
    val record = richProducer.props.partitionizer(element.value) match {
      case Some(partitionId) => new ProducerRecord(richProducer.props.topic, partitionId, element.key, element.value)
      case None => new ProducerRecord(richProducer.props.topic, element.key, element.value)
    }
    richProducer.producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
        if (exception != null) {
          handleError(exception)
        }
      }
    })

    ()
  }

  private def handleError(ex: Throwable) = {
    log.error(ex, "Stopping Kafka subscriber due to fatal error.")
    stop()
  }

  def stop() = {
    context.stop(self)
  }

  override def postStop(): Unit = {
    cleanupResources()
    super.postStop()
  }

  def cleanupResources(): Unit = richProducer.producer.close(closeTimeoutMs, TimeUnit.MILLISECONDS)
}
