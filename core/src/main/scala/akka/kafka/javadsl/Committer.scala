/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl
import java.util.concurrent.CompletionStage

import akka.Done
import akka.kafka.ConsumerMessage.Committable
import akka.kafka.{scaladsl, CommitterSettings}
import akka.stream.javadsl.Sink

import scala.compat.java8.FutureConverters.FutureOps

object Committer {

  def sink(settings: CommitterSettings): Sink[Committable, CompletionStage[Done]] =
    scaladsl.Committer.sink(settings).mapMaterializedValue(_.toJava).asJava

}
