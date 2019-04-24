/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

// #oneToMany
import akka.Done
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.ProducerMessage.Envelope
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{KafkaPorts, ProducerMessage, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

// #oneToMany

class AtLeastOnce extends DocsSpecBase(KafkaPorts.DockerKafkaPort) {

  override val bootstrapServers: String = KafkaPorts.DockerKafkaBootstrapServers

  override def sleepAfterProduce: FiniteDuration = 10.seconds

  "Connect a Consumer to Producer" should "map messages one-to-many, and commit in batches" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic1 = createCleanTopic(1)
    val topic2 = createCleanTopic(2)
    val topic3 = createCleanTopic(3)
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
        .toMat(Committer.sink(committerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
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
    val topic1 = createCleanTopic(1)
    val topic2 = createCleanTopic(2)
    val topic3 = createCleanTopic(3)
    val topic4 = createCleanTopic(4)
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
        .toMat(Committer.sink(committerSettings))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
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
}
