package com.softwaremill.react.kafka

import akka.stream.scaladsl.{PublisherSink, Source}
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

  def createHelperPublisher(elements: Long): Publisher[String] =
    Source(createLazyStream(elements)).runWith(PublisherSink())


  def createLazyStream(i: Long): Stream[String] = {
    if (i == 0)
      Stream.empty
    else
      Stream.cons(i.toString, createLazyStream(i - 1))
  }

  override def createErrorStatePublisher(): Publisher[String] = {
    val publisher = kafka.consume("error_topic", "groupId")
    publisher.asInstanceOf[ReactiveKafkaPublisher].consumer.close()
    publisher
  }

}
