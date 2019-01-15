/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.annotation.InternalApi
import akka.kafka.scaladsl.Consumer._
import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] abstract class KafkaSourceStage[K, V, Msg](stageName: String)
    extends GraphStageWithMaterializedValue[SourceShape[Msg], Control] {
  protected val out = Outlet[Msg]("out")
  val shape = new SourceShape(out)

  override protected def initialAttributes: Attributes = Attributes.name(stageName)

  protected def logic(shape: SourceShape[Msg]): GraphStageLogic with Control
  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val result = logic(shape)
    (result, result)
  }
}
