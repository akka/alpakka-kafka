package com.softwaremill.react.kafka

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorRef
import akka.stream.scaladsl.Sink
import akka.testkit.{TestKit, TestProbe}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.fixture.Suite

import scala.collection.JavaConverters._
import scala.concurrent.duration._
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
      val buffer: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
      source
        .map(_.value())
        .take(msgs.length.toLong)
        .runWith(Sink.foreach(str => { buffer.add(str); () }))
      Thread.sleep(3000) // read messages into buffer
      buffer.asScala.toSeq.sorted == msgs.sorted
    }

  def killActorWith(actor: ActorRef, msg: Any) = {
    val probe = TestProbe()
    probe.watch(actor)
    actor ! msg
    probe.expectTerminated(actor, max = 6 seconds)
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
