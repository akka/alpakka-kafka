/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.atomic.AtomicReference

import akka.Done
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.kafka.scaladsl.{Consumer, Transactional}
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import akka.kafka.{KafkaPorts, ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionsExample extends DocsSpecBase(KafkaPorts.ScalaTransactionsExamples) with EmbeddedKafkaLike {

  override def sleepAfterProduce: FiniteDuration = 10.seconds

  "Transactional sink" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val producerSettings = producerDefaults
    val sourceTopic = createTopic(1)
    val sinkTopic = createTopic(2)
    val transactionalId = createTransactionalId()
    // #transactionalSink
    val control =
      Transactional
        .source(consumerSettings, Subscriptions.topics(sourceTopic))
        .via(businessFlow)
        .map { msg =>
          ProducerMessage.single(new ProducerRecord(sinkTopic, msg.record.key, msg.record.value), msg.partitionOffset)
        }
        .toMat(Transactional.sink(producerSettings, transactionalId))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

    // ...

    // #transactionalSink
    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(sinkTopic))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(sourceTopic, 1 to 10))
    control.shutdown().futureValue should be(Done)
    control2.shutdown().futureValue should be(Done)
    // #transactionalSink
    control.drainAndShutdown()
    // #transactionalSink
    result.futureValue should have size (10)
  }

  it should "support `withContext`" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val producerSettings = producerDefaults
    val sourceTopic = createTopic(1)
    val sinkTopic = createTopic(2)
    val transactionalId = createTransactionalId()
    val control =
      Transactional
        .sourceWithContext(consumerSettings, Subscriptions.topics(sourceTopic))
        .via(businessFlow)
        .map { record =>
          ProducerMessage.single(new ProducerRecord(sinkTopic, record.key, record.value))
        }
        .toMat(Transactional.sinkWithContext(producerSettings, transactionalId))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(sinkTopic))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(sourceTopic, 1 to 10))
    control.shutdown().futureValue shouldBe Done
    control2.shutdown().futureValue shouldBe Done
    control.drainAndShutdown().futureValue shouldBe Done
    result.futureValue should have size 10
  }

  "TransactionsFailureRetryExample" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val producerSettings = producerDefaults
    val sourceTopic = createTopic(1)
    val sinkTopic = createTopic(2)
    val transactionalId = createTransactionalId()
    // #transactionalFailureRetry
    val innerControl = new AtomicReference[Control](Consumer.NoopControl)

    val stream = RestartSource.onFailuresWithBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    ) { () =>
      Transactional
        .source(consumerSettings, Subscriptions.topics(sourceTopic))
        .via(businessFlow)
        .map { msg =>
          ProducerMessage.single(new ProducerRecord(sinkTopic, msg.record.key, msg.record.value), msg.partitionOffset)
        }
        // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
        .mapMaterializedValue(c => innerControl.set(c))
        .via(Transactional.flow(producerSettings, transactionalId))
    }

    stream.runWith(Sink.ignore)

    // Add shutdown hook to respond to SIGTERM and gracefully shutdown stream
    sys.ShutdownHookThread {
      Await.result(innerControl.get.shutdown(), 10.seconds)
    }
    // #transactionalFailureRetry
    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(sinkTopic))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(sourceTopic, 1 to 10))
    innerControl.get.shutdown().futureValue should be(Done)
    control2.shutdown().futureValue should be(Done)
    result.futureValue should have size (10)
  }

  "withContext" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val producerSettings = producerDefaults
    val sourceTopic = createTopic(1)
    val sinkTopic = createTopic(2)
    val transactionalId = createTransactionalId()
    val control =
      Transactional
        .sourceWithContext(consumerSettings, Subscriptions.topics(sourceTopic))
        .via(businessFlow)
        .map { record =>
          ProducerMessage.single(new ProducerRecord(sinkTopic, record.key, record.value))
        }
        .toMat(Transactional.sinkWithContext(producerSettings, transactionalId))(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

    // ...

    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(sinkTopic))
      .toMat(Sink.seq)(Keep.both)
      .run()

    awaitProduce(produce(sourceTopic, 1 to 10))
    control.shutdown().futureValue should be(Done)
    control2.shutdown().futureValue should be(Done)
    // #transactionalSink
    control.drainAndShutdown()
    // #transactionalSink
    result.futureValue should have size (10)
  }

}
