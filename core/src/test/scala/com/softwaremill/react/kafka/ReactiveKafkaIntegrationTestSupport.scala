package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.ActorRef
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import com.softwaremill.react.kafka.KafkaMessages._
import org.scalatest.fixture.Suite

import scala.concurrent.duration._
import scala.language.postfixOps

trait ReactiveKafkaIntegrationTestSupport extends Suite with KafkaTest {

  def shouldCommitOffsets(storage: String)(implicit f: FixtureParam) = {
    // given
    givenQueueWithElements(Seq("0", "1", "2", "3", "4", "5"), storage)

    // when
    val consumerProps = consumerProperties(f)
      .commitInterval(100 millis)
      .noAutoCommit()
      .setProperty("offsets.storage", storage)

    val consumerWithSink = f.kafka.consumeWithOffsetSink(consumerProps)
    Source(consumerWithSink.publisher)
      .filter(_.message().toInt < 3)
      .to(consumerWithSink.offsetCommitSink).run()
    Thread.sleep(3000) // wait for flush
    consumerWithSink.cancel()
    Thread.sleep(3000) // wait for cancel

    // then
    verifyQueueHas(Seq("3", "4", "5"), storage)
  }

  def givenQueueWithElements(msgs: Seq[String], storage: String = "kafka")(implicit f: FixtureParam) = {
    val kafkaSubscriberActor = stringSubscriberActor(f)
    Source(msgs.toList).to(Sink(ActorSubscriber[String](kafkaSubscriberActor))).run()
    verifyQueueHas(msgs, storage)
    completeProducer(kafkaSubscriberActor)
  }

  def verifyQueueHas(msgs: Seq[String], storage: String = "kafka")(implicit f: FixtureParam) = {
    val consumerProps = consumerProperties(f).noAutoCommit().setProperty("offsets.storage", storage)
    val consumerActor = f.kafka.consumerActor(consumerProps)

    Source(ActorPublisher[StringKafkaMessage](consumerActor))
      .map(_.message())
      .runWith(TestSink.probe[String])
      .request(msgs.length.toLong)
      .expectNext(msgs.head, msgs.tail.head, msgs.tail.tail: _*)
    // kill the consumer
    cancelConsumer(consumerActor)
  }

  def cancelConsumer(consumerActor: ActorRef) =
    killActorWith(consumerActor, Cancel)

  def completeProducer(producerActor: ActorRef) =
    killActorWith(producerActor, OnComplete)

  def killActorWith(actor: ActorRef, msg: Any) = {
    val probe = TestProbe()
    probe.watch(actor)
    actor ! msg
    probe.expectTerminated(actor, max = 6 seconds)
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
