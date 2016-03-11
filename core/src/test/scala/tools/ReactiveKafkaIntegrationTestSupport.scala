package com.softwaremill.react.kafka.tools

import java.util.UUID

import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.softwaremill.react.kafka.ProducerProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.fixture.Suite
import test.tools.KafkaTest

import scala.language.postfixOps

trait ReactiveKafkaIntegrationTestSupport extends Suite with KafkaTest {

  this: TestKit =>
  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  def givenQueueWithElements(msgs: Seq[String])(implicit f: FixtureParam) = {
    val prodProps: ProducerProperties[String, String] = createProducerProperties(f)
    val producer = new KafkaProducer(prodProps.rawProperties, prodProps.keySerializer, prodProps.valueSerializer)
    msgs.foreach { msg =>
      producer.send(new ProducerRecord(f.topic, msg))
    }
    producer.close()
  }

  def verifyQueueHas(msgs: Seq[String])(implicit f: FixtureParam) =
    awaitCond {
      val source = createSource(f, consumerProperties(f).noAutoCommit())
      var buffer = Seq.empty[String]
      source
        .map(_.value())
        .take(msgs.length.toLong)
        .runWith(Sink.foreach(str => { buffer :+= str; () }))
      Thread.sleep(3000) // read messages into buffer
      buffer.sorted == msgs.sorted
    }

  def givenInitializedTopic()(implicit f: FixtureParam) = {
    val props = createProducerProperties(f)
    val producer = new KafkaProducer(createProducerProperties(f).rawProperties, props.keySerializer, props.valueSerializer)
    producer.send(new ProducerRecord(f.topic, InitialMsg))
    producer.close()
  }

  def withFixture(test: OneArgTest) = {
    val topic = uuid()
    val group = uuid()
    val kafka = newKafka()
    val theFixture = FixtureParam(topic, group, kafka)
    withFixture(test.toNoArgTest(theFixture))
  }

  def uuid() = UUID.randomUUID().toString

}
