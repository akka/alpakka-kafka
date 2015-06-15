package com.softwaremill.react.kafka

import java.util.UUID

import akka.stream.scaladsl.{Sink, Source}
import kafka.serializer.StringEncoder
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}
import org.reactivestreams.{Publisher, Subscriber}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class ReactiveKafkaSubscriberBlackboxSpec(defaultTimeout: FiniteDuration)
  extends SubscriberBlackboxVerification[String](new TestEnvironment(defaultTimeout.toMillis))
  with TestNGSuiteLike with ReactiveStreamsTckVerificationBase {

  def this() = this(300 millis)

  def partitionizer(in: String): Option[Array[Byte]] = Some(Option(in) getOrElse (UUID.randomUUID().toString) getBytes)
  
  override def createSubscriber(): Subscriber[String] = {
    val topic = UUID.randomUUID().toString
    kafka.publish(topic, "group", new StringEncoder(), partitionizer)
  }

  def createHelperSource(elements: Long) : Source[String, _] = elements match {
    case 0 => Source.empty
    case Long.MaxValue => Source(initialDelay = 10 millis, interval = 10 millis, tick = message)
    case n if n <= Int.MaxValue => Source(List.fill(n.toInt)(message))
    case n => sys.error("n > Int.MaxValue")
  }

  override def createHelperPublisher(elements: Long): Publisher[String] = {
    createHelperSource(elements).runWith(Sink.publisher)
  }

  override def createElement(i: Int): String = i.toString
}
