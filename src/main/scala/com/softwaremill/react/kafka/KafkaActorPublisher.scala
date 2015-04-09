package com.softwaremill.react.kafka

import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import com.softwaremill.react.kafka.KafkaActorPublisher.Poll
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}
import kafka.serializer.Decoder

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
private[kafka] class KafkaActorPublisher[T](consumer: KafkaConsumer, decoder: Decoder[T]) extends ActorPublisher[T] {

  val iterator = consumer.iterator()

  override def receive = {
    case ActorPublisherMessage.Request(_) | Poll => read()
    case ActorPublisherMessage.Cancel | ActorPublisherMessage.SubscriptionTimeoutExceeded => cleanupResources()
  }

  private def read() {
    if (totalDemand < 0 && isActive)
      onError(new IllegalStateException("3.17: Overflow"))
    else readDemandedItems()
  }

  private def tryReadingSingleElement() = {
    Try(iterator.next().message()).map(bytes => Some(decoder.fromBytes(bytes))).recover {
      // We handle timeout exceptions as normal 'end of the queue' cases
      case _: ConsumerTimeoutException => None
    }
  }

  private def readDemandedItems() {
    while (isActive && totalDemand > 0) {
      tryReadingSingleElement() match {
        case Success(None) =>
          if (totalDemand > 0)
            self ! Poll
        case Success(valueOpt) =>
          valueOpt.foreach(element => onNext(element))
        case Failure(ex) => onError(ex)
      }
    }
  }

  private def cleanupResources() {
    consumer.close()
    context.stop(self)
  }
}

private[kafka] object KafkaActorPublisher {
  case object Poll
}