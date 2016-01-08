package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages.StringConsumerRecord
import kafka.producer.ReactiveKafkaProducer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class ReactiveKafkaPublisherSpec(defaultTimeout: FiniteDuration)
    extends PublisherVerification[ConsumerRecord[String, String]](new TestEnvironment(defaultTimeout.toMillis), defaultTimeout.toMillis)
    with TestNGSuiteLike with ReactiveStreamsTckVerificationBase {

  override implicit val system: ActorSystem = ActorSystem("ReactiveKafkaPublisherSpec")

  def this() = this(30000 millis)

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
    val properties = ProducerProperties(kafkaHost, topic, serializer, serializer)
      .requestRequiredAcks(-1)
      .messageSendMaxRetries(3)
    val lowLevelProducer = new ReactiveKafkaProducer(properties)

    val record = new ProducerRecord(topic, 0, "key", "msg")
    (1L to realSize) foreach { number =>
      lowLevelProducer.producer.send(record)
    }
    val src = kafka.graphStageSource(ConsumerProperties(kafkaHost, topic, group, new StringDeserializer(), new StringDeserializer()))
    val c = Source.fromGraph(src).runWith(Sink.asPublisher(fanout = false))
    lowLevelProducer.producer.close()
    c
  }

  override def createFailedPublisher(): Publisher[ConsumerRecord[String, String]] = {
    new Publisher[ConsumerRecord[String, String]] {
      override def subscribe(subscriber: Subscriber[_ >: ConsumerRecord[String, String]]): Unit = {
        subscriber.onSubscribe(new Subscription {
          override def cancel(): Unit = {}

          override def request(l: Long): Unit = {}
        })
        subscriber.onError(new RuntimeException)
      }
    }
  }
}
