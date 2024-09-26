/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.concurrent.{CompletionStage, Executor}

import akka.Done
import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.kafka.internal.KafkaConsumerActor.Internal.{ConsumerMetrics, RequestMetrics}
import akka.kafka.{javadsl, scaladsl}
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic
import akka.util.Timeout
import org.apache.kafka.common.{Metric, MetricName}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.{CompletionStageOps, FutureOps}

private object PromiseControl {
  sealed trait ControlOperation
  case object ControlStop extends ControlOperation
  case object ControlShutdown extends ControlOperation
}

/** Internal API */
@InternalApi
private trait PromiseControl extends GraphStageLogic with scaladsl.Consumer.Control {
  import PromiseControl._

  def shape: SourceShape[_]
  def performShutdown(): Unit
  def performStop(): Unit = {
    setKeepGoing(true)
    complete(shape.out)
    onStop()
  }

  private val shutdownPromise: Promise[Done] = Promise()
  private val stopPromise: Promise[Done] = Promise()

  private val controlCallback = getAsyncCallback[ControlOperation]({
    case ControlStop => performStop()
    case ControlShutdown => performShutdown()
  })

  def onStop() =
    stopPromise.trySuccess(Done)

  def onShutdown() = {
    stopPromise.trySuccess(Done)
    shutdownPromise.trySuccess(Done)
  }

  override def stop(): Future[Done] = {
    controlCallback.invoke(ControlStop)
    stopPromise.future
  }
  override def shutdown(): Future[Done] = {
    controlCallback.invoke(ControlShutdown)
    shutdownPromise.future
  }
  override def isShutdown: Future[Done] = shutdownPromise.future

}

/** Internal API */
@InternalApi
private trait MetricsControl extends scaladsl.Consumer.Control {

  protected def executionContext: ExecutionContext
  protected def consumerFuture: Future[ActorRef]

  // FIXME: this can't be accessed until the stream has materialized because the `def executionContext` implementation
  // takes the executioncontext from the materializer. should it throw an exception, or block, until materialization?
  def metrics: Future[Map[MetricName, Metric]] = {
    import akka.pattern.ask

    import scala.concurrent.duration._
    consumerFuture
      .flatMap { consumer =>
        consumer
          .ask(RequestMetrics)(Timeout(1.minute))
          .mapTo[ConsumerMetrics]
          .map(_.metrics)(ExecutionContext.parasitic)
      }(executionContext)
  }
}

/** Internal API */
@InternalApi
final private[kafka] class ConsumerControlAsJava(underlying: scaladsl.Consumer.Control)
    extends javadsl.Consumer.Control {
  override def stop(): CompletionStage[Done] = underlying.stop().asJava

  override def shutdown(): CompletionStage[Done] = underlying.shutdown().asJava

  override def drainAndShutdown[T](streamCompletion: CompletionStage[T], ec: Executor): CompletionStage[T] =
    underlying.drainAndShutdown(streamCompletion.asScala)(ExecutionContext.fromExecutor(ec)).asJava

  override def isShutdown: CompletionStage[Done] = underlying.isShutdown.asJava

  override def getMetrics: CompletionStage[java.util.Map[MetricName, Metric]] =
    underlying.metrics.map(_.asJava)(ExecutionContext.parasitic).asJava
}

/** Internal API */
@InternalApi
private[kafka] object ConsumerControlAsJava {
  def apply(underlying: scaladsl.Consumer.Control): javadsl.Consumer.Control = new ConsumerControlAsJava(underlying)
}
