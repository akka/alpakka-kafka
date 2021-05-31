/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.kafka.internal.CommitCollectorStage
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
    Flow[Committable]
      .map(NotUsed -> _)
      .via(flowWithContext[NotUsed, NotUsed](settings, NotUsed)((_, _) => NotUsed))
      .map(_._2)
  }

  /**
   * API MAY CHANGE
   *
   * Batches offsets from context and commits them to Kafka, emits no useful value, but keeps the committed
   * `CommittableOffsetBatch` as context.
   */
  @ApiMayChange
  def flowWithContext[E, ACC](settings: CommitterSettings, seed: ACC)(
      aggregate: (ACC, E) => ACC
  ): FlowWithContext[E, Committable, ACC, CommittableOffsetBatch, NotUsed] = {
    val offsetBatches: Flow[(E, Committable), (ACC, CommittableOffsetBatch), NotUsed] =
      Flow.fromGraph(new CommitCollectorStage[E, ACC](settings, seed)(aggregate))

    // See https://github.com/akka/alpakka-kafka/issues/882
    import akka.kafka.CommitDelivery._
    val tupledFlow = settings.delivery match {
      case WaitForAck =>
        offsetBatches
          .mapAsyncUnordered(settings.parallelism) {
            case (acc, batch) =>
              batch.commitInternal().map(_ => (acc, batch))(ExecutionContexts.parasitic)
          }
      case SendAndForget =>
        offsetBatches.map {
          case (acc, batch) =>
            (acc, batch.tellCommit())
        }
    }
    FlowWithContext.fromTuples(tupledFlow)
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
  def sinkWithContext[E](settings: CommitterSettings): Sink[(E, Committable), Future[Done]] =
    Flow[(E, Committable)]
      .via(flowWithContext[E, NotUsed](settings, NotUsed)((_, _) => NotUsed))
      .toMat(Sink.ignore)(Keep.right)

}
