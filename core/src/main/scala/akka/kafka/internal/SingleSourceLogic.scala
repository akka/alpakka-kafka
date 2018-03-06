/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.event.Logging
import akka.event.Logging.LogLevel
import akka.kafka.Subscriptions._
import akka.kafka.{ConsumerFailed, ConsumerSettings, KafkaConsumerActor, Subscription}
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{ActorMaterializerHelper, SourceShape}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec

private[kafka] abstract class SingleSourceLogic[K, V, Msg](
    val shape: SourceShape[Msg],
    settings: ConsumerSettings[K, V],
    subscription: Subscription
) extends GraphStageLogic(shape) with PromiseControl with MessageBuilder[K, V, Msg] with StageLogging {

  override protected def logSource: Class[_] = classOf[SingleSourceLogic[K, V, Msg]]

  var consumer: ActorRef = _
  var self: StageActor = _
  var tps = Set.empty[TopicPartition]
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
  var requested = false
  var requestId = 0
  var shutdownStarted = false
  val partitionLogLevel = if (settings.wakeupDebug) Logging.InfoLevel else Logging.DebugLevel

  override def preStart(): Unit = {
    super.preStart()

    consumer = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      val name = s"kafka-consumer-${KafkaConsumerActor.Internal.nextNumber()}"
      extendedActorSystem.systemActorOf(KafkaConsumerActor.props(settings), name)
    }

    self = getStageActor {
      case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
        // might be more than one in flight when we assign/revoke tps
        if (msg.requestId == requestId)
          requested = false
        // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
        if (buffer.hasNext) {
          buffer = buffer ++ msg.messages
        }
        else {
          buffer = msg.messages
        }
        pump()
      case (_, Terminated(ref)) if ref == consumer =>
        failStage(new ConsumerFailed)
    }
    self.watch(consumer)

    val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { newTps =>
      // Is info too much here? Will be logged at startup / rebalance
      tps ++= newTps
      log.log(partitionLogLevel, "Assigned partitions: {}. All partitions: {}", newTps, tps)
      requestMessages()
    }

    val partitionRevokedCB = getAsyncCallback[Set[TopicPartition]] { newTps =>
      tps --= newTps
      log.log(partitionLogLevel, "Revoked partitions: {}. All partitions: {}", newTps, tps)
      requestMessages()
    }

    def rebalanceListener =
      KafkaConsumerActor.rebalanceListener(tps => partitionAssignedCB.invoke(tps), partitionRevokedCB.invoke)

    subscription match {
      case TopicSubscription(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener), self.ref)
      case TopicSubscriptionPattern(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener), self.ref)
      case Assignment(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.Assign(topics), self.ref)
        tps ++= topics
      case AssignmentWithOffset(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.AssignWithOffset(topics), self.ref)
        tps ++= topics.keySet
      case AssignmentOffsetsForTimes(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.AssignOffsetsForTimes(topics), self.ref)
        tps ++= topics.keySet
    }

  }

  @tailrec
  private def pump(): Unit = {
    if (isAvailable(shape.out)) {
      if (buffer.hasNext) {
        val msg = buffer.next()
        push(shape.out, createMessage(msg))
        pump()
      }
      else if (!requested && tps.nonEmpty) {
        requestMessages()
      }
    }
  }

  private def requestMessages(): Unit = {
    requested = true
    requestId += 1
    log.debug("Requesting messages, requestId: {}, partitions: {}", requestId, tps)
    consumer.tell(KafkaConsumerActor.Internal.RequestMessages(requestId, tps), self.ref)
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = {
      pump()
    }

    override def onDownstreamFinish(): Unit = {
      performShutdown()
    }
  })

  override def postStop(): Unit = {
    consumer ! KafkaConsumerActor.Internal.Stop
    onShutdown()
    super.postStop()
  }

  override def performShutdown(): Unit = {
    setKeepGoing(true)
    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    self.become {
      case (_, Terminated(ref)) if ref == consumer =>
        onShutdown()
        completeStage()
    }
    consumer ! KafkaConsumerActor.Internal.Stop
  }
}
