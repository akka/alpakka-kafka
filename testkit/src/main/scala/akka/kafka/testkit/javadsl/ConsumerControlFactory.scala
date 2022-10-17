/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl

import java.util.concurrent.{CompletableFuture, CompletionStage, Executor}

import akka.Done
import akka.annotation.ApiMayChange
import akka.kafka.javadsl.Consumer
import akka.stream.javadsl.{Flow, Keep, Source}
import akka.stream.{scaladsl, KillSwitch, KillSwitches}
import org.apache.kafka.common.{Metric, MetricName}

/**
 * Helper factory to create [[akka.kafka.javadsl.Consumer.Control]] instances when
 * testing without a Kafka broker.
 */
@ApiMayChange
object ConsumerControlFactory {

  def attachControl[A, B](source: Source[A, B]): Source[A, Consumer.Control] =
    source
      .viaMat(controlFlow(), Keep.right[B, Consumer.Control])

  def controlFlow[A](): Flow[A, A, Consumer.Control] =
    scaladsl
      .Flow[A]
      .viaMat(KillSwitches.single[A])(scaladsl.Keep.right)
      .mapMaterializedValue(killSwitch => control(killSwitch))
      .asJava

  def control(killSwitch: KillSwitch): Consumer.Control = new FakeControl(killSwitch)

  class FakeControl(val killSwitch: KillSwitch) extends Consumer.Control {

    val shutdownPromise: CompletableFuture[Done] = new CompletableFuture[Done]()

    override def stop(): CompletionStage[Done] = {
      killSwitch.shutdown()
      shutdownPromise.complete(Done)
      shutdownPromise
    }

    override def shutdown(): CompletionStage[Done] = stop()

    override def isShutdown: CompletionStage[Done] = shutdownPromise

    override def getMetrics: CompletionStage[java.util.Map[MetricName, Metric]] = ???

    override def drainAndShutdown[T](
        streamCompletion: CompletionStage[T],
        ec: Executor
    ): CompletionStage[T] =
      stop().thenCompose(new java.util.function.Function[Done, CompletionStage[T]] {
        override def apply(t: Done): CompletionStage[T] = streamCompletion
      })

  }

}
