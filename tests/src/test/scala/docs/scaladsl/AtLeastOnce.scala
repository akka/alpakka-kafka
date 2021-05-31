/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

// #oneToMany
import akka.{Done, NotUsed}
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.ProducerMessage.Envelope
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

// #oneToMany

class AtLeastOnce extends DocsSpecBase with TestcontainersKafkaLike {

  override def sleepAfterProduce: FiniteDuration = 10.seconds

  "Connect a Consumer to Producer" should "map messages one-to-many, and commit in batches" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic1 = createTopic(1)
    val topic2 = createTopic(2)
    val topic3 = createTopic(3)
    val producerSettings = producerDefaults
    val committerSettings = committerDefaults
    val control =
      // #oneToMany
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(
          msg =>
            ProducerMessage.multi(
              immutable.Seq(
                new ProducerRecord(topic2, msg.record.key, msg.record.value),
                new ProducerRecord(topic3, msg.record.key, msg.record.value)
              ),
              msg.committableOffset
            )
        )
        .via(Producer.flexiFlow(producerSettings))
        .map(_.passThrough)
        .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
        .run()
    // #oneToMany
    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic2, topic3))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(topic1, 1 to 10))
    Await.result(control.drainAndShutdown(), 5.seconds) should be(Done)
    Await.result(control2.shutdown(), 5.seconds) should be(Done)
    result.futureValue should have size (20)
  }

  "At-Least-Once One To Conditional" should "work" in assertAllStagesStopped {

    def duplicate(value: String): Boolean = "1" == value
    def ignore(value: String): Boolean = "2" == value

    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic1 = createTopic(1)
    val topic2 = createTopic(2)
    val topic3 = createTopic(3)
    val topic4 = createTopic(4)
    val producerSettings = producerDefaults
    val committerSettings = committerDefaults
    val control =
      // #oneToConditional
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topic1))
        .map(msg => {
          val out: Envelope[String, String, CommittableOffset] =
            if (duplicate(msg.record.value))
              ProducerMessage.multi(
                immutable.Seq(
                  new ProducerRecord(topic2, msg.record.key, msg.record.value),
                  new ProducerRecord(topic3, msg.record.key, msg.record.value)
                ),
                msg.committableOffset
              )
            else if (ignore(msg.record.value))
              ProducerMessage.passThrough(msg.committableOffset)
            else
              ProducerMessage.single(
                new ProducerRecord(topic4, msg.record.key, msg.record.value),
                msg.committableOffset
              )
          out
        })
        .via(Producer.flexiFlow(producerSettings))
        .map(_.passThrough)
        .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
        .run()
    // #oneToConditional

    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic2, topic3, topic4))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(topic1, 1 to 10))
    Await.result(control.drainAndShutdown(), 5.seconds) should be(Done)
    Await.result(control2.shutdown(), 5.seconds) should be(Done)
    result.futureValue should have size (10)
  }

  it should "support `withOffsetContext`" in assertAllStagesStopped {

    def duplicate(value: String): Boolean = "1" == value
    def ignore(value: String): Boolean = "2" == value

    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic1 = createTopic(1)
    val topic2 = createTopic(2)
    val topic3 = createTopic(3)
    val topic4 = createTopic(4)
    val producerSettings = producerDefaults
    val committerSettings = committerDefaults
    val control =
      Consumer
        .sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic1))
        .map(record => {
          val out: Envelope[String, String, NotUsed] =
            if (duplicate(record.value))
              ProducerMessage.multi(
                immutable.Seq(
                  new ProducerRecord(topic2, record.key, record.value),
                  new ProducerRecord(topic3, record.key, record.value)
                )
              )
            else if (ignore(record.value))
              ProducerMessage.passThrough()
            else
              ProducerMessage.single(
                new ProducerRecord(topic4, record.key, record.value)
              )
          out
        })
        .via(Producer.flowWithContext(producerSettings))
        .toMat(Committer.sinkWithContext(committerSettings))(DrainingControl.apply)
        .run()

    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic2, topic3, topic4))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(topic1, 1 to 10))
    Await.result(control.drainAndShutdown(), 5.seconds) should be(Done)
    Await.result(control2.shutdown(), 5.seconds) should be(Done)
    result.futureValue should have size (10)
  }

  it should "support batching of offsets `withOffsetContext`" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic1 = createTopic(1)
    val topic2 = createTopic(2)
    val control =
      Consumer
        .sourceWithOffsetContext(consumerSettings, Subscriptions.topics(topic1))
        .grouped(5)
        .map { records =>
          val key = records.head.key()
          val value = records.map(_.value()).mkString(",")
          ProducerMessage.single(
            new ProducerRecord(topic2, key, value)
          )
        }
        .mapContext(CommittableOffsetBatch(_))
        .via(Producer.flowWithContext(producerDefaults))
        .toMat(Committer.sinkWithContext(committerDefaults))(DrainingControl.apply)
        .run()

    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic2))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(topic1, 1 to 10))
    control.drainAndShutdown().futureValue shouldBe Done
    control2.shutdown().futureValue shouldBe Done
    result.futureValue should have size (2)
  }
}
