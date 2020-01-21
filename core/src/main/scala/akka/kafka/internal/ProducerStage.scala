/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicInteger

import akka.annotation.InternalApi
import akka.kafka.ProducerMessage._
import akka.kafka.ProducerSettings
import akka.stream._

import scala.concurrent.Future

/**
 * INTERNAL API
 *
 * Implemented by [[DefaultProducerStage]] and [[TransactionalProducerStage]].
 */
@InternalApi
private[internal] trait ProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]] {
  val settings: ProducerSettings[K, V]

  val in: Inlet[IN] = Inlet[IN]("messages")
  val out: Outlet[Future[OUT]] = Outlet[Future[OUT]]("result")
  val shape: FlowShape[IN, Future[OUT]] = FlowShape(in, out)
}

/**
 * INTERNAL API
 */
@InternalApi
private[internal] object ProducerStage {

  trait ProducerCompletionState {
    def onCompletionSuccess(): Unit
    def onCompletionFailure(ex: Throwable): Unit
  }
}
