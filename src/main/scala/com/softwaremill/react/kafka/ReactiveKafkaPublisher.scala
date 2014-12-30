package com.softwaremill.react.kafka

import kafka.consumer.KafkaConsumer
import org.reactivestreams.{Subscriber, Publisher}

import scala.concurrent.stm.Ref

private[kafka] class ReactiveKafkaPublisher(consumer: KafkaConsumer) extends Publisher[String] {

  val subscribers = Ref(Set[Subscriber[_ >: String]]())

  override def subscribe(subscriber: Subscriber[_ >: String]): Unit = {
    subscribers.single.getAndTransform(_ + subscriber) match {
      case subs if subs.contains(subscriber) =>
        throw new IllegalStateException(s"Subscriber=$subscriber is already subscribed to this publisher.")
      case _ =>
        val subscription = new KafkaTopicSubscription(consumer, subscriber)
        subscriber.onSubscribe(subscription)
    }
  }
}
