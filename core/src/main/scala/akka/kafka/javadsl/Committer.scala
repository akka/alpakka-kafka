/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl
import java.util.concurrent.CompletionStage

import akka.annotation.ApiMayChange
import akka.japi.Pair
import akka.{Done, NotUsed}
import akka.kafka.ConsumerMessage.Committable
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
   * API MAY CHANGE
   *
   * Batches offsets and commits them to Kafka, emits `Done` for every committed batch.
   */
  @ApiMayChange
  def flowWithContext[E](settings: CommitterSettings): FlowWithContext[E, Committable, Done, Done, NotUsed] =
    scaladsl.Committer.flowWithContext(settings).asJava

  /**
   * Batches offsets and commits them to Kafka.
   */
  def sink[C <: Committable](settings: CommitterSettings): Sink[C, CompletionStage[Done]] =
    scaladsl.Committer.sink(settings).mapMaterializedValue(_.toJava).asJava

  /**
   * API MAY CHANGE
   *
   * Batches offsets and commits them to Kafka.
   */
  @ApiMayChange
  def sinkWithContext[E, C <: Committable](settings: CommitterSettings): Sink[Pair[E, C], CompletionStage[Done]] =
    akka.stream.scaladsl
      .Flow[Pair[E, C]]
      .map(_.toScala)
      .via(flowWithContext(settings))
      .toMat(akka.stream.scaladsl.Sink.ignore)(akka.stream.scaladsl.Keep.right)
      .mapMaterializedValue[CompletionStage[Done]](_.toJava)
      .asJava[Pair[E, C]]
}
