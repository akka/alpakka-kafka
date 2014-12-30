package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{PublisherSink, Source}
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}
import org.reactivestreams.{Publisher, Subscriber}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class KafkaSubscriberBlackboxSpec(defaultTimeout: FiniteDuration) extends SubscriberBlackboxVerification[String](
  new TestEnvironment(defaultTimeout.toMillis)) with TestNGSuiteLike {

  def this() = this(300 millis)

  implicit val system = ActorSystem()
  implicit val mat = FlowMaterializer()

  val kafka = new ReactiveKafka("192.168.0.10:9092")
  val message = "foo"

  //    val publisher = kafka.consume("kci", "grupa")
  //    val subscriber: Subscriber[String] = kafka.publish("kci", "grupa")
  override def createSubscriber(): Subscriber[String] = {
    kafka.publish("kci", "grupa")
  }

  def createHelperSource(elements: Long): Source[String] = elements match {
    /** if `elements` is 0 the `Publisher` should signal `onComplete` immediately. */
    case 0 => Source.empty()
    /** if `elements` is [[Long.MaxValue]] the produced stream must be infinite. */
    case Long.MaxValue => Source(() => List(message).iterator)
    /** It must create a `Publisher` for a stream with exactly the given number of elements. */
    case n if n <= Int.MaxValue => Source(List.fill(n.toInt)(message))
    /** I assume that the number of elements is always less or equal to [[Int.MaxValue]] */
    case n => sys.error("n > Int.MaxValue")
  }
  override def createHelperPublisher(elements: Long): Publisher[String] = {
    createHelperSource(elements).runWith(PublisherSink())
  }
}
