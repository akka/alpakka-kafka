/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{ActorRef, Status, Terminated}
import akka.annotation.InternalApi
import akka.kafka.Subscriptions.{Assignment, AssignmentOffsetsForTimes, AssignmentWithOffset}
import akka.kafka.{ConsumerFailed, ManualSubscription}
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{AsyncCallback, GraphStageLogic, OutHandler}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/**
 * Internal API.
 *
 * Shared GraphStageLogic for [[SingleSourceLogic]] and [[ExternalSingleSourceLogic]].
 */
@InternalApi private abstract class BaseSingleSourceLogic[K, V, Msg](
    val shape: SourceShape[Msg]
) extends GraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with StageIdLogging
    with SourceLogicSubscription
    with MessageBuilder[K, V, Msg]
    with SourceLogicBuffer[K, V, Msg] {

  override protected def executionContext: ExecutionContext = materializer.executionContext
  protected def consumerFuture: Future[ActorRef]
  protected final var consumerActor: ActorRef = _
  protected var sourceActor: StageActor = _
  protected var tps = Set.empty[TopicPartition]
  private var requested = false
  private var requestId = 0

  private val assignedCB: AsyncCallback[Set[TopicPartition]] = getAsyncCallback[Set[TopicPartition]] { assignedTps =>
    tps ++= assignedTps
    log.debug("Assigned partitions: {}. All partitions: {}", assignedTps, tps)
    requestMessages()
  }

  private val revokedCB: AsyncCallback[Set[TopicPartition]] = getAsyncCallback[Set[TopicPartition]] { revokedTps =>
    tps --= revokedTps
    log.debug("Revoked partitions: {}. All partitions: {}", revokedTps, tps)
  }

  override def preStart(): Unit = {
    super.preStart()

    sourceActor = getStageActor(messageHandling)
    log.info("Starting. StageActor {}", sourceActor.ref)
    consumerActor = createConsumerActor()
    sourceActor.watch(consumerActor)

    configureSubscription(assignedCB, revokedCB)
  }

  protected def messageHandling: PartialFunction[(ActorRef, Any), Unit] = {
    case (_, msg: KafkaConsumerActor.Internal.Messages[K @unchecked, V @unchecked]) =>
      // might be more than one in flight when we assign/revoke tps
      if (msg.requestId == requestId)
        requested = false
      buffer = buffer ++ msg.messages
      pump()
    case (_, Status.Failure(e)) =>
      failStage(e)
    case (_, Terminated(ref)) if ref == consumerActor =>
      failStage(new ConsumerFailed())
  }

  protected def createConsumerActor(): ActorRef

  override protected def configureManualSubscription(subscription: ManualSubscription): Unit = subscription match {
    case Assignment(topics) =>
      consumerActor.tell(KafkaConsumerActor.Internal.Assign(topics), sourceActor.ref)
      tps ++= topics
    case AssignmentWithOffset(topics) =>
      consumerActor.tell(KafkaConsumerActor.Internal.AssignWithOffset(topics), sourceActor.ref)
      tps ++= topics.keySet
    case AssignmentOffsetsForTimes(topics) =>
      consumerActor.tell(KafkaConsumerActor.Internal.AssignOffsetsForTimes(topics), sourceActor.ref)
      tps ++= topics.keySet
  }

  @tailrec
  private def pump(): Unit =
    if (isAvailable(shape.out)) {
      if (buffer.hasNext) {
        val msg = buffer.next()
        push(shape.out, createMessage(msg))
        pump()
      } else if (!requested && tps.nonEmpty) {
        requestMessages()
      }
    }

  protected def requestMessages(): Unit = {
    requested = true
    requestId += 1
    log.debug("Requesting messages, requestId: {}, partitions: {}", requestId, tps)
    consumerActor.tell(KafkaConsumerActor.Internal.RequestMessages(requestId, tps), sourceActor.ref)
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = pump()
    override def onDownstreamFinish(cause: Throwable): Unit =
      performShutdown()
  })

  override def postStop(): Unit = {
    onShutdown()
    super.postStop()
  }

  def performShutdown(): Unit =
    log.info("Completing")
}
