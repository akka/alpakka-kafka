package com.softwaremill.react.kafka

import com.softwaremill.react.kafka.RichKafkaConsumer._
import kafka.consumer.KafkaConsumer
import org.reactivestreams.{Subscriber, Subscription}

import scala.util.control.NonFatal
private[kafka] class KafkaTopicSubscription(consumer: KafkaConsumer,
                                            subscriber: Subscriber[_ >: String]) extends Subscription {

  override def request(n: Long) {
    require(n > 0, "Rule 3.9: n <= 0")
    if (consumer.connected()) {
      (1L to n.toInt) foreach { _ =>
        consumer.read(bytes => {
          val msgAsStr = new String(bytes)
          subscriber.onNext(msgAsStr)
        })
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
