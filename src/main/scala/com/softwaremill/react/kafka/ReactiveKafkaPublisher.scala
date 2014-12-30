package com.softwaremill.react.kafka

import java.util.concurrent.TimeUnit

import kafka.consumer.KafkaConsumer
import org.I0Itec.zkclient.ZkClient
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.stm.Ref
import scala.util.control.NonFatal

private[kafka] class ReactiveKafkaPublisher(val consumer: KafkaConsumer) extends Publisher[String] {

  val subscribers = Ref(Set[Subscriber[_ >: String]]())

  override def subscribe(subscriber: Subscriber[_ >: String]): Unit = {

    subscribers.single.getAndTransform(_ + subscriber) match {
      case subs if subs.contains(subscriber) =>
        throw new IllegalStateException(s"Subscriber=$subscriber is already subscribed to this publisher.")
      case _ =>
        try {
          if (!connected()) throw new IllegalStateException("1.4 Publisher not connected")
          val subscription = new KafkaTopicSubscription(consumer, subscriber)
          subscriber.onSubscribe(subscription)
        } catch {
          case NonFatal(exception) => subscriber.onError(exception)
        }
    }
  }

  def connected() = {
    consumer.connector.getClass.getField("zkClient").setAccessible(true)
    val zkClient = consumer.connector.getClass.getField("zkClient").get(consumer.connector).asInstanceOf[ZkClient]
    zkClient.waitUntilConnected(1, TimeUnit.MILLISECONDS)
  }
}
