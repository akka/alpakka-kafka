/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicInteger

import akka.annotation.InternalApi
import akka.kafka.ProducerMessage._
import akka.stream._
import akka.stream.stage._
import org.apache.kafka.clients.producer.Producer

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] trait ProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]] {
  val closeTimeout: FiniteDuration
  val closeProducerOnStop: Boolean
  val producerProvider: () => Producer[K, V]

  val in: Inlet[IN] = Inlet[IN]("messages")
  val out: Outlet[Future[OUT]] = Outlet[Future[OUT]]("result")
  val shape: FlowShape[IN, Future[OUT]] = FlowShape(in, out)
}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] object ProducerStage {

  trait ProducerCompletionState {
    def onCompletionSuccess(): Unit
    def onCompletionFailure(ex: Throwable): Unit
  }

  trait MessageCallback[K, V, P] {
    def awaitingConfirmation: AtomicInteger
    def onMessageAckCb: AsyncCallback[Envelope[K, V, P]]
  }
}
