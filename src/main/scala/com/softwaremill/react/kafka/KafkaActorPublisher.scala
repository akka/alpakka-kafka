package com.softwaremill.react.kafka

import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import com.softwaremill.react.kafka.KafkaActorPublisher.Poll
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}
import kafka.message.MessageAndMetadata

import scala.annotation.tailrec
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

private[kafka] class KafkaActorPublisher[K, V](consumer: KafkaConsumer[K, V])
    extends ActorPublisher[KeyValueKafkaMessage[K, V]] {

  val iterator = consumer.iterator()

  override def receive = {
    case ActorPublisherMessage.Request(_) | Poll => readDemandedItems()
    case ActorPublisherMessage.Cancel | ActorPublisherMessage.SubscriptionTimeoutExceeded =>
      cleanupResources()
      context.stop(self)
  }

  private def demand_? : Boolean = totalDemand > 0

  override def postStop(): Unit = {
    cleanupResources()
    super.postStop()
  }

  private def tryReadingSingleElement(): Try[Option[KeyValueKafkaMessage[K, V]]] = {
    Try {
      val msgOpt = if (iterator.hasNext() && demand_?) Option(iterator.next()) else None
      msgOpt.map(KeyValueKafkaMessage(_))
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
      case Failure(ex) => onError(ex)
    }
  }

  private def cleanupResources(): Unit = {
    consumer.close()
  }
}

private[kafka] object KafkaActorPublisher {
  case object Poll
}

case class KeyValueKafkaMessage[K, V](msgAndMetadata: MessageAndMetadata[K, V]) {
  def key: K = msgAndMetadata.key()
  def msg: V = msgAndMetadata.message()
}

object KafkaMessage {
  type KafkaMessage[M] = KeyValueKafkaMessage[Array[Byte], M]
  type StringKafkaMessage = KafkaMessage[String]
}