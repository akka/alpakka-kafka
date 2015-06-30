package com.softwaremill.react.kafka

import java.util.UUID

import kafka.serializer.StringEncoder
import org.reactivestreams.{Subscription, Subscriber}
import org.reactivestreams.tck.SubscriberWhiteboxVerification.{SubscriberPuppet, WhiteboxSubscriberProbe}
import org.reactivestreams.tck.{SubscriberWhiteboxVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class ReactiveKafkaSubscriberWhiteboxSpec(defaultTimeout: FiniteDuration)
    extends SubscriberWhiteboxVerification[String](new TestEnvironment(defaultTimeout.toMillis))
    with TestNGSuiteLike with ReactiveStreamsTckVerificationBase {

  def this() = this(300 millis)

  override def createSubscriber(whiteboxSubscriberProbe: WhiteboxSubscriberProbe[String]): Subscriber[String] = {
    val topic = UUID.randomUUID().toString
    new SubscriberDecorator(kafka.publish(topic, "group", new StringEncoder()), whiteboxSubscriberProbe)
  }

  override def createElement(i: Int) = i.toString
}

class SubscriberDecorator[T](decoratee: Subscriber[T], probe: WhiteboxSubscriberProbe[T]) extends Subscriber[T] {

  override def onSubscribe(subscription: Subscription): Unit = {
    decoratee.onSubscribe(subscription)

    // register a successful Subscription, and create a Puppet,
    // for the WhiteboxVerification to be able to drive its tests:
    probe.registerOnSubscribe(new SubscriberPuppet() {
      override def triggerRequest(elements: Long) {
        subscription.request(elements)
      }

      override def signalCancel() {
        subscription.cancel()
      }
    })
  }

  override def onNext(t: T): Unit = {
    decoratee.onNext(t)
    probe.registerOnNext(t)
  }

  override def onError(throwable: Throwable): Unit = {
    decoratee.onError(throwable)
    probe.registerOnError(throwable)
  }

  override def onComplete(): Unit = {
    decoratee.onComplete()
    probe.registerOnComplete()
  }
}