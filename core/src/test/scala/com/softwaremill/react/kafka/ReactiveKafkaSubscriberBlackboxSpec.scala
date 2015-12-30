package com.softwaremill.react.kafka

import java.util.UUID

import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages.StringProducerMessage
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}
import org.reactivestreams.{Publisher, Subscriber}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps
import scala.util.Random

class ReactiveKafkaSubscriberBlackboxSpec(defaultTimeout: FiniteDuration)
    extends SubscriberBlackboxVerification[StringProducerMessage](new TestEnvironment(defaultTimeout.toMillis))
    with TestNGSuiteLike with ReactiveStreamsTckVerificationBase {

  def this() = this(300 millis)

  def partitionizer(in: String): Option[Int] = Some(Option(in.toInt) getOrElse Random.nextInt())

  override def createSubscriber(): Subscriber[StringProducerMessage] = {
    val topic = UUID.randomUUID().toString
    val partitionizerProvider: (String) => Option[Int] = partitionizer
    kafka.publish(ProducerProperties(kafkaHost, topic, serializer, serializer, partitionizerProvider))
  }

  def createHelperSource(elements: Long): Source[StringProducerMessage, _] = elements match {
    case 0 => Source.empty
    case Long.MaxValue => Source(initialDelay = 10 millis, interval = 10 millis, tick = ProducerMessage(message, message))
    case n if n <= Int.MaxValue => Source(List.fill(n.toInt)(ProducerMessage(message, message)))
    case n => sys.error("n > Int.MaxValue")
  }

  override def createHelperPublisher(elements: Long): Publisher[StringProducerMessage] = {
    createHelperSource(elements).runWith(Sink.publisher)
  }

  override def createElement(i: Int): StringProducerMessage = ProducerMessage(i.toString, i.toString)
}
