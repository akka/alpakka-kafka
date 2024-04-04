/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.ProducerMessage._
import akka.kafka.ProducerSettings
import akka.kafka.internal.ProducerStage.ProducerCompletionState
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Supervision}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] class DefaultProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
    val settings: ProducerSettings[K, V]
) extends GraphStage[FlowShape[IN, Future[OUT]]]
    with ProducerStage[K, V, P, IN, OUT] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new DefaultProducerStageLogic(this, inheritedAttributes)
}

/**
 * Internal API.
 *
 * Used by [[DefaultProducerStage]], extended by [[TransactionalProducerStageLogic]].
 */
private class DefaultProducerStageLogic[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
    stage: ProducerStage[K, V, P, IN, OUT],
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with StageIdLogging
    with DeferredProducer[K, V]
    with ProducerCompletionState {

  private lazy val decider: Decider =
    inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
  private var awaitingConfirmation = 0
  private var completionState: Option[Try[Done]] = None

  override protected def logSource: Class[_] = classOf[DefaultProducerStage[_, _, _, _, _]]

  final override val producerSettings: ProducerSettings[K, V] = stage.settings

  protected def awaitingConfirmationValue: Int = awaitingConfirmation

  protected class DefaultInHandler extends InHandler {
    override def onPush(): Unit = produce(grab(stage.in))

    override def onUpstreamFinish(): Unit = {
      completionState = Some(Success(Done))
      checkForCompletion()
    }

    override def onUpstreamFailure(ex: Throwable): Unit = {
      completionState = Some(Failure(ex))
      checkForCompletion()
    }
  }

  override def preStart(): Unit = {
    resolveProducer(stage.settings)
  }

  protected def checkForCompletion(): Unit =
    if (isClosed(stage.in) && awaitingConfirmation == 0) {
      completionState match {
        case Some(Success(_)) => onCompletionSuccess()
        case Some(Failure(ex)) => onCompletionFailure(ex)
        case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
      }
    }

  override def onCompletionSuccess(): Unit = if (readyToShutdown()) completeStage()

  override def onCompletionFailure(ex: Throwable): Unit = failStage(ex)

  private val confirmAndCheckForCompletionCB: AsyncCallback[Unit] = getAsyncCallback[Unit] { _ =>
    awaitingConfirmation -= 1
    checkForCompletion()
  }

  override protected val closeAndFailStageCb: AsyncCallback[Throwable] = getAsyncCallback[Throwable] { ex =>
    closeProducerImmediately()
    failStage(ex)
  }

  protected def postSend(msg: Envelope[K, V, P]): Unit = ()

  override protected def producerAssigned(): Unit = resumeDemand()

  protected def resumeDemand(tryToPull: Boolean = true): Unit = {
    log.debug("Resume demand")
    setHandler(
      stage.out,
      new OutHandler {
        override def onPull(): Unit = tryPull(stage.in)

        override def onDownstreamFinish(cause: Throwable): Unit = {
          super.onDownstreamFinish(cause)
        }
      }
    )
    // kick off demand for more messages if we're resuming demand
    if (tryToPull && isAvailable(stage.out) && !hasBeenPulled(stage.in)) {
      tryPull(stage.in)
    }
  }

  protected def suspendDemand(): Unit = {
    log.debug("Suspend demand")
    suspendDemandOutHandler()
  }

  // factored out of suspendDemand because logging is not permitted when called from the stage logic constructor
  private def suspendDemandOutHandler(): Unit = {
    setHandler(
      stage.out,
      new OutHandler {
        override def onPull(): Unit = ()
      }
    )
  }

  protected def initialInHandler(): Unit = producingInHandler()
  protected def producingInHandler(): Unit = setHandler(stage.in, new DefaultInHandler())

  // suspend demand until a Producer has been created
  suspendDemandOutHandler()
  initialInHandler()

  protected def produce(in: Envelope[K, V, P]): Unit =
    in match {
      case msg: Message[K, V, P] =>
        val r = Promise[Result[K, V, P]]()
        awaitingConfirmation += 1
        producer.send(msg.record, new SendCallback(msg, r))
        postSend(msg)
        val future = r.future.asInstanceOf[Future[OUT]]
        push(stage.out, future)

      case multiMsg: MultiMessage[K, V, P] =>
        val promises = for {
          msg <- multiMsg.records
        } yield {
          val r = Promise[MultiResultPart[K, V]]()
          awaitingConfirmation += 1
          producer.send(msg, new SendMultiCallback(msg, r))
          r.future
        }
        postSend(multiMsg)
        implicit val ec: ExecutionContext = this.materializer.executionContext
        val res = Future.sequence(promises).map { parts =>
          MultiResult(parts, multiMsg.passThrough)
        }
        val future = res.asInstanceOf[Future[OUT]]
        push(stage.out, future)

      case passthrough: PassThroughMessage[K, V, P] =>
        postSend(passthrough)
        val future = Future.successful(PassThroughResult(in.passThrough)).asInstanceOf[Future[OUT]]
        push(stage.out, future)

    }

  private abstract class CallbackBase(promise: Promise[_]) extends Callback {
    protected def emitElement(metadata: RecordMetadata): Unit

    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
      if (exception == null) {
        emitElement(metadata)
        confirmAndCheckForCompletionCB.invoke(())
      } else
        decider(exception) match {
          case Supervision.Stop => closeAndFailStageCb.invoke(exception)
          case _ =>
            promise.failure(exception)
            confirmAndCheckForCompletionCB.invoke(())
        }
  }

  /** send-callback for a single message. */
  private final class SendCallback(msg: Message[K, V, P], promise: Promise[Result[K, V, P]])
      extends CallbackBase(promise) {
    override protected def emitElement(metadata: RecordMetadata): Unit =
      promise.success(Result(metadata, msg))
  }

  /** send-callback for a multi-message. */
  private final class SendMultiCallback(msg: ProducerRecord[K, V], promise: Promise[MultiResultPart[K, V]])
      extends CallbackBase(promise) {
    override protected def emitElement(metadata: RecordMetadata): Unit =
      promise.success(MultiResultPart(metadata, msg))
  }

  override def postStop(): Unit = {
    log.debug("ProducerStage postStop")
    closeProducer()
  }

  // Specifically for transactional producer that needs to defer shutdown to let an async task
  // complete before actually shutting down
  protected def readyToShutdown(): Boolean = true
}
