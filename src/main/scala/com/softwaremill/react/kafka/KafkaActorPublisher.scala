package com.softwaremill.react.kafka

import java.util.concurrent.atomic.AtomicReference

import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}
import kafka.message.MessageAndMetadata
import org.reactivestreams.Subscription

import scala.util.{Failure, Success, Try}
import RichKafkaConsumer._
private[kafka] class KafkaActorPublisher(consumer: KafkaConsumer) extends ActorPublisher[String] {

  val iterator = consumer.stream.iterator()

  private def generateElement() = {
    Try(iterator.next().message()).map(bytes => Some(new String(bytes))).recover {
      // We handle timeout exceptions as normal 'end of the queue' cases
      case _: ConsumerTimeoutException => None
    }
  }

  val subscription: Subscription = null

  override def receive = {
    case ActorPublisherMessage.Request(_) => read()
    case ActorPublisherMessage.Cancel | ActorPublisherMessage.SubscriptionTimeoutExceeded => cleanupResources()
    case ClosePublisher =>
      consumer.close()
      sender ! "ok"
  }

  private def read() {
    while (isActive && totalDemand > 0) {
      generateElement() match {
        case Success(valueOpt) =>
          valueOpt
            .map(element => onNext(element))
            .getOrElse(onComplete())
        case Failure(ex) =>
          onError(ex)
      }
    }
  }

  private def cleanupResources() {
    // TODO
  }
}

private[kafka] case object ClosePublisher