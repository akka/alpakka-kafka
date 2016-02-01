package com.softwaremill.react.kafka

import akka.actor.ActorLogging
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import com.softwaremill.react.kafka.KafkaActorPublisher.{CommitAck, CommitOffsets, Poll}
import com.softwaremill.react.kafka.commit.OffsetMap
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.collection.JavaConversions._
import scala.annotation.tailrec
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

private[kafka] class KafkaActorPublisher[K, V](consumerAndProps: ReactiveKafkaConsumer[K, V])
    extends ActorPublisher[ConsumerRecord[K, V]] with ActorLogging {

  val pollTimeoutMs = consumerAndProps.properties.pollTimeout.toMillis
  val consumer = consumerAndProps.consumer
  var buffer: Option[java.util.Iterator[ConsumerRecord[K, V]]] = None
  var closed = false

  override def receive = {
    case ActorPublisherMessage.Request(_) | Poll if isActive => readDemandedItems()
    case ActorPublisherMessage.SubscriptionTimeoutExceeded => context.stop(self)
    case CommitOffsets(offsets) => if (isActive && !closed) runCommit(offsets)
  }

  private def getIterator(): Iterator[ConsumerRecord[K, V]] = {
    buffer match {
      case Some(iterator) =>
        iterator
      case None =>
        if (!closed)
          consumer.poll(pollTimeoutMs).iterator()
        else
          Iterator.empty
    }
  }

  private def runCommit(offsets: OffsetMap): Unit = {
    try {
      consumer.commitSync(offsets.toCommitRequestInfo)
      log.debug(s"committed offsets: $offsets")
      sender() ! CommitAck
    }
    catch {
      case ex: Exception => onErrorThenStop(ex)
    }
  }

  @tailrec
  private def readDemandedItems(): Unit = {
    Try(getIterator()) match {
      case Success(iterator) =>
        if (!iterator.hasNext && demand_?)
          self ! Poll
        else {
          while (iterator.hasNext && demand_?) {
            val record = iterator.next()
            onNext(record)
          }
          buffer = None
          if (demand_?) {
            // nothing more in iterator but still some demand
            readDemandedItems()
          }
          else if (iterator.hasNext) {
            // no demand but data left, let's buffer whatever's left
            buffer = Some(iterator)
          }
        }
      case Failure(ex) => onErrorThenStop(ex)
    }
  }

  private def demand_? : Boolean = totalDemand > 0

  private def cleanupResources(): Unit = {
    if (!closed) {
      closed = true
      consumer.close()
    }
  }

  override def postStop() = {
    cleanupResources()
  }
}

object KafkaActorPublisher {
  private[kafka] case object Poll
  case class CommitOffsets(offsets: OffsetMap)
  case class CommitAck(offsets: OffsetMap)
  case object Stop
}

object KafkaMessages {
  type StringConsumerRecord = ConsumerRecord[Array[Byte], String]
  type StringProducerMessage = ProducerMessage[Array[Byte], String]
}