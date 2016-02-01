package com.softwaremill.react.kafka

import java.util.UUID

import akka.stream.scaladsl.{Sink, Source}
import kafka.serializer.StringEncoder
import org.reactivestreams.Subscriber
import org.reactivestreams.tck.SubscriberWhiteboxVerification.WhiteboxSubscriberProbe
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
    val props = ProducerProperties(kafkaHost, topic, "group", new StringEncoder())
    val sinkStage = kafka.graphStageSink(props)
    val sub = Source.asSubscriber.to(Sink.fromGraph(sinkStage)).run()
    new SubscriberDecorator(sub, whiteboxSubscriberProbe)
  }

  override def createElement(i: Int) = i.toString
}
