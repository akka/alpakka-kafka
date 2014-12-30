package com.softwaremill.react.kafka

import akka.stream.scaladsl.{Source, PublisherSink}
import org.reactivestreams.Publisher
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class ReactiveKafkaPublisherSpec(defaultTimeout: FiniteDuration)
  extends PublisherVerification[String](new TestEnvironment(defaultTimeout.toMillis), defaultTimeout.toMillis)
  with ReactiveStreamsTckVerificationBase {

  def this() = this(300 millis)

  override def createPublisher(l: Long) = {
    createHelperPublisher(l)
  }

  def createHelperSource(elements: Long): Source[String] = elements match {
    case 0 => Source.empty()
    case Long.MaxValue => Source(() => List(message).iterator)
    case n if n <= Int.MaxValue => Source(List.fill(n.toInt)(message))
    case n => sys.error("n > Int.MaxValue")
  }

  def createHelperPublisher(elements: Long): Publisher[String] = {
    createHelperSource(elements).runWith(PublisherSink())
  }

  override def createErrorStatePublisher(): Publisher[String] = {
    kafka.consume("error_topic", "groupId")
  }
}
