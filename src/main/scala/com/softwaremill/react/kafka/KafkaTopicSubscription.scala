package com.softwaremill.react.kafka

import com.softwaremill.react.kafka.RichKafkaConsumer._
import kafka.consumer.KafkaConsumer
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.stm.{Ref, atomic}
import scala.util.control.NonFatal

private[kafka] class KafkaTopicSubscription(consumer: KafkaConsumer,
                                            subscriber: Subscriber[_ >: String]) extends Subscription {

  val demand = Ref(0L)

  override def request(n: Long) {
    require(n > 0, "Rule 3.9: n <= 0")
    if (consumer.connected()) {
      atomic { implicit txn =>
        demand.transformAndGet(_ + n) match {
          case exceededDemand if exceededDemand < 0 => // 3.17: overflow
            try consumer.close() catch {
              case NonFatal(_) => // mute
            }
            subscriber.onError(new IllegalStateException("Rule 3.17: Pending + n > Long.MaxValue"))
          case numberToPull =>
            val numberOfPulledItems = consumer.readElems(numberToPull, bytes => {
              val msgAsStr = new String(bytes)
              subscriber.onNext(msgAsStr)
            })
            demand -= numberOfPulledItems
        }
      }
    } // else 3.6 NOP
  }

  override def cancel() = try {
    consumer.close()
  } catch {
    case NonFatal(exception) =>
      subscriber.onError(new IllegalStateException("onComplete threw an exception", exception))
  }
}
