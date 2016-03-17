/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.fixture.Suite
import scala.collection.JavaConverters._
import scala.language.postfixOps
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.Matchers

trait ReactiveKafkaIntegrationTestSupport extends Suite with KafkaTest
    with Matchers with ConversionCheckedTripleEquals {

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
    awaitAssert {
      val source = createSource(f, consumerProperties(f).noAutoCommit())
      var buffer = Seq.empty[String]
      source
        .map(_.value())
        .take(msgs.length.toLong)
        .runWith(Sink.foreach(str => { buffer :+= str; () }))
      Thread.sleep(3000) // read messages into buffer
      buffer.sorted should ===(msgs.sorted)
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
