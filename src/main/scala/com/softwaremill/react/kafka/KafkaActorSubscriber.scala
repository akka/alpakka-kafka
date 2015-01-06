package com.softwaremill.react.kafka

import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}
import kafka.producer.KafkaProducer

private[kafka] class KafkaActorSubscriber(val producer: KafkaProducer) extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  def receive = {
    case ActorSubscriberMessage.OnNext(element) =>
      processElement(element.asInstanceOf[String])
    case ActorSubscriberMessage.OnError(ex) =>
      handleError(ex)
    case ActorSubscriberMessage.OnComplete =>
      streamFinished()
  }

  private def processElement(element: String) = {
    producer.send(element)
  }

  private def handleError(ex: Throwable) = {
    producer.close()
  }

  private def streamFinished() = {
    producer.close()
  }
}
