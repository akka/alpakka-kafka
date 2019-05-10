/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicInteger

import akka.annotation.InternalApi
import akka.kafka.ProducerMessage._
import akka.stream._
import org.apache.kafka.clients.producer.Producer

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * INTERNAL API
 *
 * Implemented by [[DefaultProducerStage]] and [[TransactionalProducerStage]].
 */
@InternalApi
private[internal] trait ProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P], S] {
  val closeTimeout: FiniteDuration
  val closeProducerOnStop: Boolean
  val producerProvider: S => Producer[K, V]

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

  trait MessageCallback[K, V, P] {
    protected def awaitingConfirmation: AtomicInteger
  }
}
