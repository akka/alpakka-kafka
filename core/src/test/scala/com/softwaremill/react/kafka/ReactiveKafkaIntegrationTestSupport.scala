package com.softwaremill.react.kafka

import java.util.UUID

import akka.actor.{PoisonPill, ActorRef}
import akka.stream.ClosedShape
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.{FlushCompleted, AddFlushListener}
import scala.collection.immutable.Seq
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{TestKitBase, TestProbe}
import com.softwaremill.react.kafka.KafkaMessages._
import org.scalatest.fixture.Suite

import scala.concurrent.duration._
import scala.language.postfixOps

trait ReactiveKafkaIntegrationTestSupport extends Suite with KafkaTest with TestKitBase {

  def shouldCommitOffsets(storage: String)(implicit f: FixtureParam) = {
    // given
    givenQueueWithElements(Seq("0", "1", "2", "3", "4", "5"), storage)

    // when
    val consumerProps = consumerProperties(f)
      .commitInterval(100 millis)
      .noAutoCommit()
      .setProperty("offsets.storage", storage)

    val consumerWithSink = f.kafka.consumeWithOffsetSink(consumerProps)
    val probe = TestProbe()
    consumerWithSink.kafkaOffsetCommitSink.underlyingCommitterActor ! AddFlushListener(probe.testActor)

    val src = Source.fromPublisher(consumerWithSink.publisher)

    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[KafkaMessage[String]](2, eagerCancel = true))
      src ~> bcast.in

      bcast.out(0).filter { m => m.message().toInt < 3 } ~> consumerWithSink.offsetCommitSink
      // on the second exit drop all, just plug into a "cancellator" actor
      bcast.out(1).filter(_ => false) ~> Sink.actorRef(probe.testActor, "completed")
      ClosedShape
    })
    g.run()
    probe.expectMsg(max = 30 seconds, FlushCompleted)
    probe.testActor ! PoisonPill // cancel the stream when we are sure that commit flush has been completed

    // then
    verifyQueueHas(Seq("3", "4", "5"), storage)
  }

  def givenQueueWithElements(msgs: Seq[String], storage: String = "kafka")(implicit f: FixtureParam) = {
    val kafkaSubscriberActor = stringSubscriberActor(f)
    Source(msgs.toList).to(Sink.fromSubscriber(ActorSubscriber[String](kafkaSubscriberActor))).run()
    verifyQueueHas(msgs, storage)
    completeProducer(kafkaSubscriberActor)
  }

  def verifyQueueHas(msgs: Seq[String], storage: String = "kafka")(implicit f: FixtureParam) = {
    val consumerProps = consumerProperties(f).noAutoCommit().setProperty("offsets.storage", storage)
    val consumerActor = f.kafka.consumerActor(consumerProps)

    Source.fromPublisher(ActorPublisher[StringKafkaMessage](consumerActor))
      .map(_.message())
      .runWith(TestSink.probe[String])
      .request(msgs.length.toLong)
      .expectNextN(msgs)
      .cancel()
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
