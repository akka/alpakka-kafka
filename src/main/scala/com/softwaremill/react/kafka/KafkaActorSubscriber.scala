package com.softwaremill.react.kafka

import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import kafka.producer.KafkaProducer
import kafka.serializer.{Encoder, Decoder}

private[kafka] class KafkaActorSubscriber[T](val producer: KafkaProducer, val encoder: Encoder[T]) extends ActorSubscriber {

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
    producer.send(encoder.toBytes(element), None)
  }

  private def handleError(ex: Throwable) = {
    producer.close()
  }

  private def streamFinished() = {
    producer.close()
  }
}
