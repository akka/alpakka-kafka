/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.kafka.internal.BatchingFlowStage
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

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
    val offsetBatches: Flow[Committable, CommittableOffsetBatch, NotUsed] =
      Flow
        .fromGraph(new BatchingFlowStage(settings))

    // See https://github.com/akka/alpakka-kafka/issues/882
    import akka.kafka.CommitDelivery._
    settings.delivery match {
      case WaitForAck =>
        offsetBatches
          .mapAsyncUnordered(settings.parallelism) { batch =>
            batch.commitInternal().map(_ => batch)(ExecutionContexts.sameThreadExecutionContext)
          }
      case SendAndForget =>
        offsetBatches.map(_.tellCommit())
    }
  }

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
