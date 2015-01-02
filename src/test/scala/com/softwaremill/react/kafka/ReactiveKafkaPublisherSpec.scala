package com.softwaremill.react.kafka

import java.util.UUID

import akka.stream.scaladsl.{PublisherSink, Source}
import com.typesafe.scalalogging.slf4j.StrictLogging
import ly.stealth.testing.BaseSpec
import org.apache.kafka.clients.producer.ProducerRecord
import org.reactivestreams.Publisher
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps
class ReactiveKafkaPublisherSpec(defaultTimeout: FiniteDuration)
  extends PublisherVerification[String](new TestEnvironment(defaultTimeout.toMillis), defaultTimeout.toMillis)
  with TestNGSuiteLike with ReactiveStreamsTckVerificationBase with StrictLogging with BaseSpec {

  def this() = this(300 millis)

  override def createPublisher(l: Long) = {
    val topic = UUID.randomUUID().toString
    val lowLevelProducer = createNewKafkaProducer("localhost:9092")
    (1L to l) foreach { number =>
      val record = new ProducerRecord(topic, 0, "key".getBytes, number.toString.getBytes)
      lowLevelProducer.send(record)
    }
    kafka.consume(topic, "group1").asInstanceOf[ReactiveKafkaPublisher]
  }

  override def createErrorStatePublisher(): Publisher[String] = {
    val publisher = kafka.consume("error_topic", "groupId")
    publisher.asInstanceOf[ReactiveKafkaPublisher].consumer.close()
    publisher
  }

}
