/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl
import akka.Done
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

object Committer {

  def sink(settings: CommitterSettings): Sink[Committable, Future[Done]] =
    Flow[Committable]
    // Not very efficient, ideally we should merge offsets instead of grouping them
      .groupedWeightedWithin(settings.maxBatch, settings.maxInterval)(_.batchSize)
      .map(CommittableOffsetBatch.apply)
      .mapAsync(1)(_.commitScaladsl())
      .toMat(Sink.ignore)(Keep.right)

}
