/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.dispatch.ExecutionContexts
import akka.annotation.ApiMayChange
import akka.{Done, NotUsed}
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink}
import akka.util.Collections

import scala.collection.immutable
import scala.collection.immutable.LinearSeq
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object Committer {

  /**
   * Batches offsets and commits them to Kafka, emits `Done` for every committed batch.
   */
  def flow(settings: CommitterSettings): Flow[Committable, Done, NotUsed] =
    batchFlow(settings).map(_ => Done)

  /**
   * Batches offsets and commits them to Kafka, emits `CommittableOffsetBatch` for every committed batch.
   */
  def batchFlow(settings: CommitterSettings): Flow[Committable, CommittableOffsetBatch, NotUsed] = {
    val fallbackFlow = Flow[Committable]
      .groupedWeightedWithin(settings.maxBatch, settings.maxInterval)(_.batchSize)
      .map(offsets => Success(FlushableOffsetBatch(CommittableOffsetBatch(offsets), flush = false)))

    val offsetBatches = if (settings.flushPartialBatches) {
      partialBatchFlushingFlow(settings)
    } else {
      fallbackFlow
    }

    // See https://github.com/akka/alpakka-kafka/issues/882
    import akka.kafka.CommitDelivery._
    settings.delivery match {
      case WaitForAck =>
        offsetBatches
          .mapAsyncUnordered(settings.parallelism) {
            case Success(b) =>
              b.batch.commitInternal(b.flush).map(_ => b.batch)(ExecutionContexts.sameThreadExecutionContext)
            case Failure(ex) => throw ex // re-failing the stream
          }
      case SendAndForget =>
        offsetBatches
          .map {
            case Success(b) =>
              b.batch.tellCommit(b.flush)
            case Failure(ex) => throw ex // re-failing the stream
          }
    }
  }

  private def partialBatchFlushingFlow(
      settings: CommitterSettings
  ): Flow[Committable, Try[FlushableOffsetBatch], NotUsed] = {
    Flow[Committable]
      .map(Success(_))
      .recover { case throwable => Failure(throwable) }
      .groupedWeightedWithin(settings.maxBatch, settings.maxInterval) {
        case Failure(_) => Long.MaxValue // make it 'not fit' for the successful batch
        case Success(committable) => committable.batchSize
      }
      .mapConcat { committables =>
        val successfulCommittables = committables
          .takeWhile(_.isSuccess)
          .collect { case Success(committable) => committable }
        val failure = committables.collectFirst { case Failure(ex) => Failure(ex) }
        val hasFailures = failure.isDefined

        Success(FlushableOffsetBatch(CommittableOffsetBatch(successfulCommittables), flush = hasFailures)) :: failure.toList
      }

  }

  private final case class FlushableOffsetBatch(batch: CommittableOffsetBatch, flush: Boolean)

  /**
   * API MAY CHANGE
   *
   * Batches offsets from context and commits them to Kafka, emits no useful value, but keeps the committed
   * `CommittableOffsetBatch` as context.
   */
  @ApiMayChange
  def flowWithOffsetContext[E](
      settings: CommitterSettings
  ): FlowWithContext[E, Committable, NotUsed, CommittableOffsetBatch, NotUsed] = {
    val value = Flow[(E, Committable)]
      .map(_._2)
      .via(batchFlow(settings))
      .map(b => (NotUsed, b))
    new FlowWithContext(value)
  }

  /**
   * Batches offsets and commits them to Kafka.
   */
  def sink(settings: CommitterSettings): Sink[Committable, Future[Done]] =
    flow(settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * API MAY CHANGE
   *
   * Batches offsets from context and commits them to Kafka.
   */
  @ApiMayChange
  def sinkWithOffsetContext[E](settings: CommitterSettings): Sink[(E, Committable), Future[Done]] =
    Flow[(E, Committable)]
      .via(flowWithOffsetContext(settings))
      .toMat(Sink.ignore)(Keep.right)

}
