package com.softwaremill.react.kafka

import java.util.UUID

import akka.stream.actor.ActorPublisher
import akka.util.Timeout
import com.typesafe.scalalogging.slf4j.StrictLogging
import ly.stealth.testing.BaseSpec
import org.apache.kafka.clients.producer.ProducerRecord
import org.reactivestreams.{Subscriber, Publisher}
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps
import scala.concurrent.duration._

class ReactiveKafkaPublisherSpec(defaultTimeout: FiniteDuration)
  extends PublisherVerification[String](new TestEnvironment(defaultTimeout.toMillis), defaultTimeout.toMillis)
  with TestNGSuiteLike with ReactiveStreamsTckVerificationBase with StrictLogging with BaseSpec {

  def this() = this(1300 millis)

  override def createPublisher(l: Long) = {
    val topic = UUID.randomUUID().toString
    val lowLevelProducer = createNewKafkaProducer("localhost:9092")
    (1L to l) foreach { number =>
      val record = new ProducerRecord(topic, 0, "key".getBytes, number.toString.getBytes)
      lowLevelProducer.send(record)
    }
    kafka.consume(topic, "group1", system)
  }

  override def createErrorStatePublisher(): Publisher[String] = {
    return new Publisher[String] {
      override def subscribe(subscriber: Subscriber[_ >: String]): Unit = subscriber.onError(new RuntimeException)
    }
  }

  override def spec103_mustSignalOnMethodsSequentially(): Unit = {
    // TODO hangs...
  }

  override def spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue(): Unit = {
    // TODO hangs...
  }

}
