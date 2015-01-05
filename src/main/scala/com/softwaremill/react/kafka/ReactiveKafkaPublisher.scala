package com.softwaremill.react.kafka

import akka.actor.{Props, ActorSystem}
import com.softwaremill.react.kafka.RichKafkaConsumer._
import kafka.consumer.KafkaConsumer
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.stm.Ref
import scala.util.control.NonFatal
private[kafka] class ReactiveKafkaPublisher(val consumer: KafkaConsumer, actorSystem: ActorSystem)
  extends Publisher[String] {

  val subscribers = Ref(Set[Subscriber[_ >: String]]())

  override def subscribe(subscriber: Subscriber[_ >: String]): Unit = {

    subscribers.single.getAndTransform(_ + subscriber) match {
      case subs if subs.contains(subscriber) =>
        throw new IllegalStateException(s"Subscriber=$subscriber is already subscribed to this publisher.")
      case _ =>
        try {
          if (!consumer.connected()) throw new IllegalStateException("1.4 Publisher not connected")
          val subscription = createSubscription(subscriber)
          subscriber.onSubscribe(subscription)
        } catch {
          case NonFatal(exception) => subscriber.onError(exception)
        }
    }
  }

  private def createSubscription(subscriber: Subscriber[_ >: String]) = {
    val subscriptionActor = actorSystem.actorOf(Props(new KafkaActorSubscription(consumer, subscriber)))
    new KafkaTopicSubscription(consumer, subscriber, subscriptionActor)
  }
}
