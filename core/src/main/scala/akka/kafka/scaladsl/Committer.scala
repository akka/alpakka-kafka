/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.annotation.ApiMayChange
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffset, CommittableOffsetBatch}
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
  def batchFlow(settings: CommitterSettings): Flow[Committable, CommittableOffsetBatch, NotUsed] =
    Flow[Committable]
      .groupedWeightedWithin(settings.maxBatch, settings.maxInterval)(_.batchSize)
      .map(CommittableOffsetBatch.apply)
      .map { b =>
        b.commit()
        b
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
  ): FlowWithContext[E, CommittableOffset, NotUsed, CommittableOffsetBatch, NotUsed] = {
    val value = Flow[(E, CommittableOffset)]
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
  def sinkWithOffsetContext[E](settings: CommitterSettings): Sink[(E, CommittableOffset), Future[Done]] =
    Flow[(E, CommittableOffset)]
      .via(flowWithOffsetContext(settings))
      .toMat(Sink.ignore)(Keep.right)
}
