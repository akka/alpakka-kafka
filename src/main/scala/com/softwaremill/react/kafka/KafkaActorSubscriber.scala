package com.softwaremill.react.kafka

import akka.actor.{ActorLogging, PoisonPill}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy}
import kafka.producer.KafkaProducer

private[kafka] class KafkaActorSubscriber[T](
  val producer: KafkaProducer[T],
  props: ProducerProperties[T],
  requestStrategyProvider: () => RequestStrategy
)
    extends ActorSubscriber with ActorLogging {

  override protected val requestStrategy = requestStrategyProvider()

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
    case "Stop" => producer.close()
  }

  private def processElement(element: T) = {
    producer.send(props.encoder.toBytes(element), props.partitionizer(element))
  }

  private def handleError(ex: Throwable) = {
    log.error("Stopping Kafka subscriber due to fatal error.", ex)
    cleanupResources()
  }

  def cleanupResources() = {
    producer.close()
    self ! PoisonPill
  }
}