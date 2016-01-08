package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.ActorRef
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestProbe
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.scalatest.fixture.Suite

import scala.concurrent.duration._
import scala.language.postfixOps

trait ReactiveKafkaIntegrationTestSupport extends Suite with KafkaTest {

  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  def shouldCommitOffsets()(implicit f: FixtureParam) = {
    // given
    givenQueueWithElements(Seq("0", "1", "2", "3", "4", "5"))

    // when
    val consumerProps = consumerProperties(f)
      .commitInterval(1000 millis)

    val consumerWithSink = f.kafka.consumeWithOffsetSink(consumerProps)
    Source.fromPublisher(consumerWithSink.publisher)
      .filter(_.value().toInt < 3)
      .to(consumerWithSink.offsetCommitSink).run()
    Thread.sleep(10000) // wait for flush
    consumerWithSink.cancel()
    Thread.sleep(3000) // wait for cancel

    // then
    verifyQueueHas(Seq("3", "4", "5"))
  }

  def givenQueueWithElements(msgs: Seq[String])(implicit f: FixtureParam) = {
    val graphSink = stringGraphSink(f)
    val kafkaSink = Sink.fromGraph(graphSink)

    val (probe, _) = TestSource.probe[String]
      .map(msg => ProducerMessage(msg))
      .toMat(kafkaSink)(Keep.both)
      .run()
    msgs.foreach(probe.sendNext)
    probe.sendComplete()
    verifyQueueHas(msgs)
    ()
  }

  def verifyQueueHas(msgs: Seq[String])(implicit f: FixtureParam) = {
    val sourceStage = createSourceStage(f)
    val probe = TestSink.probe[String]
    Source.fromGraph(sourceStage)
      .map(_.value())
      .runWith(probe)
      .request(msgs.length.toLong)
      .expectNext(msgs.head, msgs.tail.head, msgs.tail.tail: _*)
      .cancel()
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
