package com.softwaremill.react.kafka

import akka.actor.Actor
import com.softwaremill.react.kafka.RichKafkaConsumer._
import kafka.consumer.{ConsumerTimeoutException, KafkaConsumer}
import org.reactivestreams.Subscriber

import scala.util.control.NonFatal
private[kafka] class KafkaActorSubscription(consumer: KafkaConsumer,
                                            subscriber: Subscriber[_ >: String]) extends Actor {
  var demand = 0L

  def cleanupResources() = {
    // TODO
  }

  override def receive = {
    case ReadElements(n) =>
      demand = demand + n
      if (exceededMaxDemand()) {
        closeAndReportError()
      }
      else self ! TryReadDemand
    case TryReadDemand => read()
  }

  private def read() {
    if (consumer.connected()) {
      try {
        consumer.readElems(demand, {

          bytes =>
            demand = demand - 1
            val msgAsStr = new String(bytes)
            subscriber.onNext(msgAsStr)
        })
      }
      catch {
        // That's a dirty way to handle the 'no more elements' option
        case _: ConsumerTimeoutException => subscriber.onComplete()
      }
    }
  }

  private def exceededMaxDemand(): Boolean = demand < 0 // 3.17

  private def closeAndReportError() = {
    try consumer.close() catch {
      case NonFatal(_) => // mute
    }
    subscriber.onError(new IllegalStateException("Rule 3.17: Pending + n > Long.MaxValue"))
  }
}

private[kafka] case class ReadElements(n: Long)

private[kafka] case object TryReadDemand