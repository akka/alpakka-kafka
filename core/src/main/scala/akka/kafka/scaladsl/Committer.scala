/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.{Done, NotUsed}
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

object Committer {

  /**
   * Batches offsets and commits them to Kafka, emits `Done` for every committed batch.
   */
  def flow(settings: CommitterSettings): Flow[Committable, Done, NotUsed] =
    Flow[Committable]
    // Not very efficient, ideally we should merge offsets instead of grouping them
      .groupedWeightedWithin(settings.maxBatch, settings.maxInterval)(_.batchSize)
      .map(CommittableOffsetBatch.apply)
      .mapAsync(settings.parallelism)(_.commitScaladsl())

  /**
    * Batches offsets and commits them to Kafka.
    */
  def sink(settings: CommitterSettings): Sink[Committable, Future[Done]] =
    flow(settings)
      .toMat(Sink.ignore)(Keep.right)

}
