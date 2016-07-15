/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy}
import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps
import scala.util.Success

object ReactiveKafkaConsumerBenchmarks extends LazyLogging {
  val streamingTimeout = 3 minutes
  type NonCommitableFixture = ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]
  type CommitableFixture = ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]

  /**
   * Creates a predefined stream, reads N elements, discarding them into a Sink.ignore. Does not commit.
   */
  def consumePlainNoKafka(fixture: NonCommitableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    meter.mark()
    val future = Source.repeat("dummy")
      .take(fixture.msgCount.toLong)
      .map {
        msg => meter.mark(); msg
      }
      .runWith(Sink.ignore)
    Await.result(future, atMost = streamingTimeout)
    logger.debug("Stream finished")
  }

  /**
   * Creates a stream and reads N elements, discarding them into a Sink.ignore. Does not commit.
   */
  def consumePlain(fixture: NonCommitableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val future = fixture.source
      .take(fixture.msgCount.toLong)
      .map {
        msg => meter.mark(); msg
      }
      .runWith(Sink.ignore)
    Await.result(future, atMost = streamingTimeout)
    logger.debug("Stream finished")
  }

  /**
   * Reads elements from Kafka source and groups them in batches of given size. Once a batch is accumulated, commits.
   */
  def consumerAtLeastOnceBatched(batchSize: Int)(fixture: CommitableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val promise = Promise[Unit]
    val control = fixture.source
      .map(_.committableOffset)
      .groupedWithin(batchSize, 1 second)
      .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) =>
        meter.mark()
        batch.updated(elem)
      })
      .mapAsync(1)({ batch =>
        logger.debug(s"Sending commit for offset ${batch.offsets()}")
        batch.commitScaladsl().map(_ => batch.offsets())(ExecutionContexts.sameThreadExecutionContext)
      })
      .toMat(Sink.foreach { msg =>
        if (msg.head._2 >= fixture.msgCount - 1)
          promise.complete(Success(()))
      })(Keep.left)
      .run()

    Await.result(promise.future, streamingTimeout)
    control.shutdown()
    logger.debug("Stream finished")
  }

  /**
   * Reads elements from Kafka source and commits each one immediately after read.
   */
  def consumeCommitAtMostOnce(fixture: CommitableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val promise = Promise[Unit]
    val control = fixture.source
      .buffer(10, OverflowStrategy.backpressure)
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
