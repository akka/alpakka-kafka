package com.softwaremill.react.kafka

import akka.stream.scaladsl.{Source, PublisherSink}
import org.reactivestreams.Publisher
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class ReactiveKafkaPublisherSpec(defaultTimeout: FiniteDuration)
  extends PublisherVerification[String](new TestEnvironment(defaultTimeout.toMillis), defaultTimeout.toMillis)
  with TestNGSuiteLike with ReactiveStreamsTckVerificationBase {

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

  override def spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue(): Unit = {
    // TODO
    fail("this test kills JVM with \"out of heap space\" errors")
  }

  override def spec317_mustSupportACumulativePendingElementCountUpToLongMaxValue(): Unit = {
    // TODO
    fail("this test kills JVM with \"out of heap space\" errors")
  }

  override def spec317_mustSupportAPendingElementCountUpToLongMaxValue(): Unit = {
    // TODO
    fail("this test kills JVM with \"out of heap space\" errors")
  }
}
