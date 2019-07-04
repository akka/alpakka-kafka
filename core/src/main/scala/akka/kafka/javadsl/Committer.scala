/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl
import java.util.concurrent.CompletionStage

import akka.annotation.ApiMayChange
import akka.japi.Pair
import akka.{Done, NotUsed}
import akka.kafka.ConsumerMessage.{Committable, CommittableOffset, CommittableOffsetBatch}
import akka.kafka.{scaladsl, CommitterSettings}
import akka.stream.javadsl.{Flow, FlowWithContext, Sink}

import scala.compat.java8.FutureConverters.FutureOps

object Committer {

  /**
   * Batches offsets and commits them to Kafka, emits `Done` for every committed batch.
   */
  def flow[C <: Committable](settings: CommitterSettings): Flow[C, Done, NotUsed] =
    scaladsl.Committer.flow(settings).asJava

  /**
   * Batches offsets and commits them to Kafka, emits `CommittableOffsetBatch` for every committed batch.
   */
  def batchFlow[C <: Committable](settings: CommitterSettings): Flow[C, CommittableOffsetBatch, NotUsed] =
    scaladsl.Committer.batchFlow(settings).asJava

  /**
   * Batches offsets and commits them to Kafka without acknowledging a successful commit callback.  Emits
   * `CommittableOffsetBatch` for every committed batch.
   */
  def batchFlowWithNoCallback[C <: Committable](settings: CommitterSettings): Flow[C, CommittableOffsetBatch, NotUsed] =
    scaladsl.Committer.batchFlowWithNoCallback(settings).asJava

  /**
   * API MAY CHANGE
   *
   * Batches offsets from context and commits them to Kafka, emits no useful value, but keeps the committed
   * `CommittableOffsetBatch` as context.
   */
  @ApiMayChange
  def flowWithOffsetContext[E, C <: CommittableOffset](
      settings: CommitterSettings
  ): FlowWithContext[E, C, NotUsed, CommittableOffsetBatch, NotUsed] =
    scaladsl.Committer.flowWithOffsetContext[E](settings).asJava

  /**
   * API MAY CHANGE
   *
   * Batches offsets and commits them to Kafka without acknowledging a successful commit callback.  Emits
   * `CommittableOffsetBatch` for every committed batch.
   */
  @ApiMayChange
  def flowWithOffsetContextAndNoCallback[E, C <: CommittableOffset](
      settings: CommitterSettings
  ): FlowWithContext[E, C, NotUsed, CommittableOffsetBatch, NotUsed] =
    scaladsl.Committer.flowWithOffsetContextAndNoCallback(settings).asJava

  /**
   * Batches offsets and commits them to Kafka.
   */
  def sink[C <: Committable](settings: CommitterSettings): Sink[C, CompletionStage[Done]] =
    scaladsl.Committer.sink(settings).mapMaterializedValue(_.toJava).asJava

  /**
   * Batches offsets and commits them to Kafka without acknowledging a successful commit callback.
   */
  def sinkWithNoCallback[C <: Committable](settings: CommitterSettings): Sink[C, CompletionStage[Done]] =
    scaladsl.Committer.sinkWithNoCallback(settings).mapMaterializedValue(_.toJava).asJava

  /**
   * API MAY CHANGE
   *
   * Batches offsets from context and commits them to Kafka.
   */
  @ApiMayChange
  def sinkWithOffsetContext[E, C <: CommittableOffset](
      settings: CommitterSettings
  ): Sink[Pair[E, C], CompletionStage[Done]] =
    akka.stream.scaladsl
      .Flow[Pair[E, C]]
      .map(_.toScala)
      .toMat(scaladsl.Committer.sinkWithOffsetContext(settings))(akka.stream.scaladsl.Keep.right)
      .mapMaterializedValue[CompletionStage[Done]](_.toJava)
      .asJava[Pair[E, C]]

  /**
   * API MAY CHANGE
   *
   * Batches offsets and commits them to Kafka without acknowledging a successful commit callback.
   */
  @ApiMayChange
  def sinkWithOffsetContextAndNoCallback[E, C <: CommittableOffset](
      settings: CommitterSettings
  ): Sink[Pair[E, C], CompletionStage[Done]] =
    akka.stream.scaladsl
      .Flow[Pair[E, C]]
      .map(_.toScala)
      .toMat(scaladsl.Committer.sinkWithOffsetContextAndNoCallback(settings))(akka.stream.scaladsl.Keep.right)
      .mapMaterializedValue[CompletionStage[Done]](_.toJava)
      .asJava[Pair[E, C]]
}
