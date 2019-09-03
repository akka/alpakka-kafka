/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.kafka.{CommitDelivery, CommitterSettings}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Committer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps
import scala.util.Success

object ReactiveKafkaConsumerBenchmarks extends LazyLogging {
  val streamingTimeout: FiniteDuration = 30 minutes
  type NonCommittableFixture = ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]
  type CommittableFixture = ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]

  /**
   * Creates a predefined stream, reads N elements, discarding them into a Sink.ignore. Does not commit.
   */
  def consumePlainNoKafka(fixture: NonCommittableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    meter.mark()
    val future = Source
      .repeat("dummy")
      .take(fixture.msgCount.toLong)
      .map { msg =>
        meter.mark(); msg
      }
      .runWith(Sink.ignore)
    Await.result(future, atMost = streamingTimeout)
    logger.debug("Stream finished")
  }

  /**
   * Creates a stream and reads N elements, discarding them into a Sink.ignore. Does not commit.
   */
  def consumePlain(fixture: NonCommittableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val future = fixture.source
      .take(fixture.msgCount.toLong)
      .map { msg =>
        meter.mark(); msg
      }
      .runWith(Sink.ignore)
    Await.result(future, atMost = streamingTimeout)
    logger.debug("Stream finished")
  }

  /**
   * Reads elements from Kafka source and commits a batch as soon as it's possible.
   */
  def consumerAtLeastOnceBatched(batchSize: Int)(fixture: CommittableFixture, meter: Meter)(implicit sys: ActorSystem,
                                                                                            mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val committerDefaults = CommitterSettings(sys)
    val promise = Promise[Unit]
    val counter = new AtomicInteger(fixture.numberOfPartitions)
    val control = fixture.source
      .map { msg =>
        meter.mark()
        msg.committableOffset
      }
      .via(Committer.batchFlow(committerDefaults.withMaxBatch(batchSize.toLong).withParallelism(3)))
      .toMat(Sink.foreach {
        _.offsets().values
          .filter(_ >= fixture.msgCount / fixture.numberOfPartitions - 1)
          .foreach(_ => if (counter.decrementAndGet() == 0) promise.complete(Success(())))
      })(Keep.left)
      .run()

    Await.result(promise.future, streamingTimeout)
    control.shutdown()
    logger.debug("Stream finished")
  }

  /**
   * Reads elements from Kafka source and commits in batches with no backpressure on committing.
   */
  def consumerCommitAndForget(
      commitBatchSize: Int
  )(fixture: CommittableFixture, meter: Meter)(implicit sys: ActorSystem, mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val committerDefaults = CommitterSettings(sys)
    val promise = Promise[Unit]
    val counter = new AtomicInteger(fixture.numberOfPartitions)
    val control = fixture.source
      .map { msg =>
        meter.mark()
        if (counter.decrementAndGet() == 0) promise.complete(Success(()))
        msg.committableOffset
      }
      .toMat(Committer.sink(committerDefaults.withDelivery(CommitDelivery.SendAndForget).withMaxBatch(commitBatchSize.toLong)))(
        Keep.both
      )
      .mapMaterializedValue(DrainingControl.apply)
      .run()

    Await.result(promise.future, streamingTimeout)
    control.drainAndShutdown()(sys.dispatcher)
    logger.debug("Stream finished")
  }

  /**
   * Reads elements from Kafka source and commits each one immediately after read.
   */
  def consumeCommitAtMostOnce(fixture: CommittableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val promise = Promise[Unit]
    val control = fixture.source
      .mapAsync(1) { m =>
        meter.mark()
        m.committableOffset.commitScaladsl().map(_ => m)(ExecutionContexts.sameThreadExecutionContext)
      }
      .toMat(Sink.foreach { msg =>
        if (msg.committableOffset.partitionOffset.offset >= fixture.msgCount - 1)
          promise.complete(Success(()))
      })(Keep.left)
      .run()

    Await.result(promise.future, streamingTimeout)
    control.shutdown()
    logger.debug("Stream finished")
  }
}
