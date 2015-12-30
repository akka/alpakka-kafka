package com.softwaremill.react.kafka

import akka.actor.ActorLogging
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy}
import kafka.producer.ReactiveKafkaProducer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

case class ProducerMessage[K, V](key: K, value: V)
object ProducerMessage {
  def apply[K, V](consumerRecord: ConsumerRecord[K, V]) =
    new ProducerMessage(consumerRecord.key(), consumerRecord.value())

  def apply[V](v: V): ProducerMessage[V, V] = ProducerMessage(v, v)
}

private[kafka] class KafkaActorSubscriber[K, V](
  val richProducer: ReactiveKafkaProducer[K, V],
  requestStrategyProvider: () => RequestStrategy
)
    extends ActorSubscriber with ActorLogging {

  override protected val requestStrategy = requestStrategyProvider()

  def receive = {
    case ActorSubscriberMessage.OnNext(element) =>
      processElement(element.asInstanceOf[ProducerMessage[K, V]])
    case ActorSubscriberMessage.OnError(ex) =>
      handleError(ex)
    case ActorSubscriberMessage.OnComplete =>
      stop()
    case "close_producer" => richProducer.producer.close()
  }

  private def processElement(element: ProducerMessage[K, V]) = {
    val record = richProducer.props.partitionizer(element.value) match {
      case Some(partitionId) => new ProducerRecord(richProducer.props.topic, partitionId, element.key, element.value)
      case None => new ProducerRecord(richProducer.props.topic, element.key, element.value)
    }
    richProducer.producer.send(record)
    ()
  }

  private def handleError(ex: Throwable) = {
    log.error(ex, "Stopping Kafka subscriber due to fatal error.")
    stop()
  }

  def stop() = {
    cleanupResources()
    context.stop(self)
  }

  def cleanupResources(): Unit = richProducer.producer.close()
}
