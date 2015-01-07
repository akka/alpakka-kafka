package com.softwaremill.react.kafka

import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}

import scala.util.{Failure, Success, Try}
private[kafka] class KafkaActorPublisher(consumer: KafkaConsumer) extends ActorPublisher[String] {

  val iterator = consumer.iterator()

  override def receive = {
    case ActorPublisherMessage.Request(_) => read()
    case ActorPublisherMessage.Cancel | ActorPublisherMessage.SubscriptionTimeoutExceeded => cleanupResources()
  }

  private def read() {
    if (totalDemand < 0 && isActive)
      onError(new IllegalStateException("3.17: Overflow"))
    else readDemandedItems()
  }

  private def tryReadingSingleElement() = {
    Try(iterator.next().message()).map(bytes => Some(new String(bytes))).recover {
      // We handle timeout exceptions as normal 'end of the queue' cases
      case _: ConsumerTimeoutException => None
    }
  }

  private def readDemandedItems() {
    var maybeMoreElements = true
    while (isActive && totalDemand > 0 && maybeMoreElements) {
      tryReadingSingleElement() match {
        case Success(None) => maybeMoreElements = false // No more elements
        case Success(valueOpt) =>
          valueOpt.foreach(element => onNext(element))
          maybeMoreElements = true
        case Failure(ex) => onError(ex)
      }
    }

  }
  private def cleanupResources() {
    consumer.close()
  }
}