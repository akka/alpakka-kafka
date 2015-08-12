package com.softwaremill.react.kafka

import java.util.UUID

import com.softwaremill.react.kafka.KafkaMessage.StringKafkaMessage
import kafka.serializer.StringDecoder
import ly.stealth.testing.BaseSpec
import org.apache.kafka.clients.producer.ProducerRecord
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class ReactiveKafkaPublisherSpec(defaultTimeout: FiniteDuration)
    extends PublisherVerification[StringKafkaMessage](new TestEnvironment(defaultTimeout.toMillis), defaultTimeout.toMillis)
    with TestNGSuiteLike with ReactiveStreamsTckVerificationBase with BaseSpec {

  def this() = this(1300 millis)

  /**
   * This indicates that our publisher cannot provide an onComplete() signal
   */
  override def maxElementsFromPublisher(): Long = Long.MaxValue

  override def createPublisher(l: Long) = {
    val topic = UUID.randomUUID().toString
    val group = "group1"

    // Filling the queue with Int.MaxValue elements takes much too long
    // Test case which verifies point 3.17 may as well fill with small amount of elements. It verifies demand overflow
    // which has nothing to do with supply size.
    val realSize = if (l == Int.MaxValue) 30 else l

    val lowLevelProducer = createNewKafkaProducer(kafkaHost)
    val record = new ProducerRecord(topic, 0, "key".getBytes, "msg".getBytes)
    (1L to realSize) foreach { number =>
      lowLevelProducer.send(record)
    }
    kafka.consume(ConsumerProperties(kafkaHost, zkHost, topic, group, new StringDecoder()))
  }

  override def createFailedPublisher(): Publisher[StringKafkaMessage] = {
    new Publisher[StringKafkaMessage] {
      override def subscribe(subscriber: Subscriber[_ >: StringKafkaMessage]): Unit = {
        subscriber.onSubscribe(new Subscription {
          override def cancel(): Unit = {}

          override def request(l: Long): Unit = {}
        })
        subscriber.onError(new RuntimeException)
      }
    }
  }

}
