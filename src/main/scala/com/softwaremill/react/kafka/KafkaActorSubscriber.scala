package com.softwaremill.react.kafka

import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy}
import kafka.producer.KafkaProducer

private[kafka] class KafkaActorSubscriber[T](
  val producer: KafkaProducer[T],
  props: ProducerProps[T],
  requestStrategyProvider: () => RequestStrategy
)
    extends ActorSubscriber {

  protected def requestStrategy = requestStrategyProvider()

  override def postStop(): Unit = {
    cleanupResources()
    super.postStop()
  }

  def receive = {
    case ActorSubscriberMessage.OnNext(element) =>
      processElement(element.asInstanceOf[T])
    case ActorSubscriberMessage.OnError(ex) =>
      handleError(ex)
    case ActorSubscriberMessage.OnComplete | "Stop" =>
      cleanupResources()
  }

  private def processElement(element: T) = {
    producer.send(props.encoder.toBytes(element), props.partitionizer(element))
  }

  private def handleError(ex: Throwable) = {
    cleanupResources()
    throw ex
  }

  def cleanupResources() = {
    producer.close()
  }
}