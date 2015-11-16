package com.softwaremill.react.kafka

import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import com.softwaremill.react.kafka.KafkaActorPublisher.Poll
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}
import kafka.message.MessageAndMetadata

import scala.annotation.tailrec
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

private[kafka] class KafkaActorPublisher[T](consumer: KafkaConsumer[T]) extends ActorPublisher[KafkaMessage[T]] {

  val iterator = consumer.iterator()

  override def receive = {
    case ActorPublisherMessage.Request(_) | Poll => readDemandedItems()
    case ActorPublisherMessage.Cancel | ActorPublisherMessage.SubscriptionTimeoutExceeded =>
      cleanupResources()
      context.stop(self)
  }

  private def demand_? : Boolean = totalDemand > 0

  private def tryReadingSingleElement(): Try[Option[KafkaMessage[T]]] = {
    Try {
      if (iterator.hasNext() && demand_?) Option(iterator.next()) else None
    } recover {
      // We handle timeout exceptions as normal 'end of the queue' cases
      case _: ConsumerTimeoutException => None
    }
  }

  @tailrec
  private def readDemandedItems(): Unit = {
    tryReadingSingleElement() match {
      case Success(None) =>
        if (demand_?) self ! Poll
      case Success(Some(element)) =>
        onNext(element)
        if (demand_?) readDemandedItems()
      case Failure(ex) =>
        cleanupResources()
        onErrorThenStop(ex)
    }
  }

  private def cleanupResources(): Unit = {
    consumer.close()
  }
}

private[kafka] object KafkaActorPublisher {
  case object Poll
}

object KafkaMessages {
  type KafkaMessage[T] = MessageAndMetadata[Array[Byte], T]
  type StringKafkaMessage = MessageAndMetadata[Array[Byte], String]
}