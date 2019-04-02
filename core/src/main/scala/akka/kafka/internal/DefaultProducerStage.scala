/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.ProducerMessage._
import akka.kafka.internal.ProducerStage.{MessageCallback, ProducerCompletionState}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.{Attributes, FlowShape, Supervision}
import akka.stream.stage._
import org.apache.kafka.clients.producer.{Callback, Producer, RecordMetadata}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] class DefaultProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
    val closeTimeout: FiniteDuration,
    val closeProducerOnStop: Boolean,
    val producerProvider: () => Producer[K, V]
) extends GraphStage[FlowShape[IN, Future[OUT]]]
    with ProducerStage[K, V, P, IN, OUT] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new DefaultProducerStageLogic(this, producerProvider(), inheritedAttributes)
}

/**
 * Internal API.
 *
 * Used by [[DefaultProducerStage]], extended by [[TransactionalProducerStageLogic]].
 */
private class DefaultProducerStageLogic[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
    stage: ProducerStage[K, V, P, IN, OUT],
    producer: Producer[K, V],
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with StageLogging
    with MessageCallback[K, V, P]
    with ProducerCompletionState {

  private lazy val decider: Decider =
    inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
  protected val awaitingConfirmation = new AtomicInteger(0)
  private var inIsClosed = false
  private var completionState: Option[Try[Done]] = None

  override protected def logSource: Class[_] = classOf[DefaultProducerStage[_, _, _, _, _]]

  def checkForCompletion(): Unit =
    if (isClosed(stage.in) && awaitingConfirmation.get == 0) {
      completionState match {
        case Some(Success(_)) => onCompletionSuccess()
        case Some(Failure(ex)) => onCompletionFailure(ex)
        case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
      }
    }

  override def onCompletionSuccess(): Unit = completeStage()

  override def onCompletionFailure(ex: Throwable): Unit = failStage(ex)

  val checkForCompletionCB: AsyncCallback[Unit] = getAsyncCallback[Unit] { _ =>
    checkForCompletion()
  }

  val failStageCb: AsyncCallback[Throwable] = getAsyncCallback[Throwable] { ex =>
    failStage(ex)
  }

  override val onMessageAckCb: AsyncCallback[Envelope[K, V, P]] = getAsyncCallback[Envelope[K, V, P]] { _ =>
    }

  def postSend(msg: Envelope[K, V, P]) = ()

  setHandler(stage.out, new OutHandler {
    override def onPull(): Unit = tryPull(stage.in)
  })

  setHandler(
    stage.in,
    new InHandler {
      override def onPush(): Unit = produce(grab(stage.in))

      override def onUpstreamFinish(): Unit = {
        inIsClosed = true
        completionState = Some(Success(Done))
        checkForCompletion()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        inIsClosed = true
        completionState = Some(Failure(ex))
        checkForCompletion()
      }
    }
  )

  def produce(in: Envelope[K, V, P]): Unit =
    in match {
      case msg: Message[K, V, P] =>
        val r = Promise[Result[K, V, P]]
        awaitingConfirmation.incrementAndGet()
        producer.send(msg.record, sendCallback(r, onSuccess = metadata => {
          onMessageAckCb.invoke(msg)
          r.success(Result(metadata, msg))
        }))
        postSend(msg)
        val future = r.future.asInstanceOf[Future[OUT]]
        push(stage.out, future)

      case multiMsg: MultiMessage[K, V, P] =>
        val promises = for {
          msg <- multiMsg.records
        } yield {
          val r = Promise[MultiResultPart[K, V]]
          awaitingConfirmation.incrementAndGet()
          producer.send(msg, sendCallback(r, onSuccess = metadata => r.success(MultiResultPart(metadata, msg))))
          r.future
        }
        postSend(multiMsg)
        implicit val ec: ExecutionContext = this.materializer.executionContext
        val res = Future.sequence(promises).map { parts =>
          onMessageAckCb.invoke(multiMsg)
          MultiResult(parts, multiMsg.passThrough)
        }
        val future = res.asInstanceOf[Future[OUT]]
        push(stage.out, future)

      case _: PassThroughMessage[K, V, P] =>
        onMessageAckCb.invoke(in)
        val future = Future.successful(PassThroughResult[K, V, P](in.passThrough)).asInstanceOf[Future[OUT]]
        push(stage.out, future)

    }

  private def sendCallback(promise: Promise[_], onSuccess: RecordMetadata => Unit): Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) onSuccess(metadata)
      else
        decider(exception) match {
          case Supervision.Stop =>
            if (stage.closeProducerOnStop) {
              producer.close(0, TimeUnit.MILLISECONDS)
            }
            failStageCb.invoke(exception)
          case _ =>
            promise.failure(exception)
        }
      if (awaitingConfirmation.decrementAndGet() == 0 && inIsClosed)
        checkForCompletionCB.invoke(())
    }
  }

  override def postStop(): Unit = {
    log.debug("Stage completed")

    if (stage.closeProducerOnStop) {
      try {
        // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
        producer.flush()
        producer.close(stage.closeTimeout.toMillis, TimeUnit.MILLISECONDS)
        log.debug("Producer closed")
      } catch {
        case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
      }
    }

    super.postStop()
  }
}
