package com.softwaremill.react.kafka

import kafka.consumer.KafkaConsumer
import org.reactivestreams.{Subscription, Subscriber}

import scala.util.control.NonFatal

private[kafka] class KafkaTopicSubscription(consumer: KafkaConsumer,
                                            subscriber: Subscriber[_ >: String]) extends Subscription {

  override def request(n: Long): Unit = {
    require(n > 0, "n <= 0")

    for (i <- 0 to n.toInt)
      consumer.read(bytes => {
        val msgAsStr = new String(bytes)
        subscriber.onNext(msgAsStr)
      })
  }

  override def cancel() = try {
    consumer.close()
  } catch {
    case NonFatal(exception) =>
      subscriber.onError(new IllegalStateException("onComplete threw an exception", exception))
  }
}
