package com.softwaremill.react.kafka

import akka.actor.ActorLogging
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import kafka.producer.KafkaProducer
import kafka.serializer.Encoder

private[kafka] class KafkaActorSubscriber[T](
  val producer: KafkaProducer,
  val encoder: Encoder[T],
  partitionizer: T => Option[Array[Byte]] = (_: T) => None
)
    extends ActorSubscriber with ActorLogging {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  def receive = {
    case ActorSubscriberMessage.OnNext(element) =>
      processElement(element.asInstanceOf[T])
    case ActorSubscriberMessage.OnError(ex) =>
      handleError(ex)
    case ActorSubscriberMessage.OnComplete =>
      streamFinished()
  }

  private def processElement(element: T) = {
    try {
      producer.send(encoder.toBytes(element), partitionizer(element))
    }
    catch {
      case e: Exception => handleError(e)
    }
  }

  private def handleError(ex: Throwable) = {
    log.error("Stopping subscriber due to an error", ex)
    producer.close()
    context.stop(self)
  }

  private def streamFinished() = {
    producer.close()
  }
}
