/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.event.Logging
import akka.kafka.Subscriptions._
import akka.kafka._
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{ActorMaterializerHelper, SourceShape}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}

private[kafka] abstract class SingleSourceLogic[K, V, Msg](
    val shape: SourceShape[Msg],
    settings: ConsumerSettings[K, V],
    subscription: Subscription
) extends GraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with MessageBuilder[K, V, Msg]
    with StageLogging {

  override protected def logSource: Class[_] = classOf[SingleSourceLogic[K, V, Msg]]

  val consumerPromise = Promise[ActorRef]
  final val actorNumber = KafkaConsumerActor.Internal.nextNumber()
  override def executionContext: ExecutionContext = materializer.executionContext
  override def consumerFuture: Future[ActorRef] = consumerPromise.future
  var consumerActor: ActorRef = _
  var sourceActor: StageActor = _
  var tps = Set.empty[TopicPartition]
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
  var requested = false
  var requestId = 0
  var shutdownStarted = false
  val partitionLogLevel = if (settings.wakeupDebug) Logging.InfoLevel else Logging.DebugLevel

  override def preStart(): Unit = {
    super.preStart()

    consumerActor = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      extendedActorSystem.systemActorOf(akka.kafka.KafkaConsumerActor.props(settings), s"kafka-consumer-$actorNumber")
    }
    consumerPromise.success(consumerActor)

    sourceActor = getStageActor {
      case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
        // might be more than one in flight when we assign/revoke tps
        if (msg.requestId == requestId)
          requested = false
        // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
        if (buffer.hasNext) {
          buffer = buffer ++ msg.messages
        } else {
          buffer = msg.messages
        }
        pump()
      case (_, Terminated(ref)) if ref == consumerActor =>
        failStage(new ConsumerFailed())
    }
    sourceActor.watch(consumerActor)

    val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { assignedTps =>
      tps ++= assignedTps
      log.log(partitionLogLevel, "Assigned partitions: {}. All partitions: {}", assignedTps, tps)
      subscription.rebalanceListener.foreach {
        _.tell(TopicPartitionsAssigned(subscription, assignedTps), sourceActor.ref)
      }
      requestMessages()
    }

    val partitionRevokedCB = getAsyncCallback[Set[TopicPartition]] { revokedTps =>
      tps --= revokedTps
      log.log(partitionLogLevel, "Revoked partitions: {}. All partitions: {}", revokedTps, tps)
      subscription.rebalanceListener.foreach {
        _.tell(TopicPartitionsRevoked(subscription, revokedTps), sourceActor.ref)
      }
    }

    def rebalanceListener: KafkaConsumerActor.ListenerCallbacks =
      KafkaConsumerActor.ListenerCallbacks(partitionAssignedCB.invoke, partitionRevokedCB.invoke)

    subscription match {
      case TopicSubscription(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener), sourceActor.ref)
      case TopicSubscriptionPattern(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener), sourceActor.ref)
      case Assignment(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.Assign(topics), sourceActor.ref)
        tps ++= topics
      case AssignmentWithOffset(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.AssignWithOffset(topics), sourceActor.ref)
        tps ++= topics.keySet
      case AssignmentOffsetsForTimes(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.AssignOffsetsForTimes(topics), sourceActor.ref)
        tps ++= topics.keySet
    }

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

  private def requestMessages(): Unit = {
    requested = true
    requestId += 1
    log.debug("Requesting messages, requestId: {}, partitions: {}", requestId, tps)
    consumerActor.tell(KafkaConsumerActor.Internal.RequestMessages(requestId, tps), sourceActor.ref)
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit =
      pump()

    override def onDownstreamFinish(): Unit =
      performShutdown()
  })

  override def postStop(): Unit = {
    consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    onShutdown()
    super.postStop()
  }

  override def performShutdown(): Unit = {
    setKeepGoing(true)
    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    sourceActor.become {
      case (_, Terminated(ref)) if ref == consumerActor =>
        onShutdown()
        completeStage()
    }
    consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
  }

}
