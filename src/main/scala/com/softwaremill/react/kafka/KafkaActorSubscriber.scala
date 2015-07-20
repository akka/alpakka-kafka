package com.softwaremill.react.kafka

import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import kafka.producer.KafkaProducer
import kafka.serializer.Encoder

private[kafka] class KafkaActorSubscriber[T](
  val producer: KafkaProducer,
  val encoder: Encoder[T],
  partitionizer: T => Option[Array[Byte]] = (_: T) => None
)
    extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  override def postStop(): Unit = {
    cleanupResources()
    super.postStop()
  }

  def receive = {
    case ActorSubscriberMessage.OnNext(element) =>
      processElement(element.asInstanceOf[T])
    case ActorSubscriberMessage.OnError(ex) =>
      handleError(ex)
    case ActorSubscriberMessage.OnComplete =>
      cleanupResources()
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
    cleanupResources()
    throw ex
  }

  def cleanupResources() = {
    producer.close()
  }
}
