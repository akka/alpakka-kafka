/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.kafka.Subscriptions.{Assignment, AssignmentWithOffset, TopicSubscription, TopicSubscriptionPattern}
import akka.kafka.{ConsumerSettings, KafkaConsumerActor, Subscription}
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{GraphStageLogic, OutHandler}
import akka.stream.{ActorMaterializerHelper, SourceShape}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec

private[kafka] abstract class SingleSourceLogic[K, V, Msg](
    val shape: SourceShape[Msg],
    settings: ConsumerSettings[K, V],
    subscription: Subscription
) extends GraphStageLogic(shape) with PromiseControl with MessageBuilder[K, V, Msg] {
  var consumer: ActorRef = _
  var self: StageActor = _
  var tps = Set.empty[TopicPartition]
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
  var requested = false
  var shutdownStarted = false

  override def preStart(): Unit = {
    super.preStart()

    consumer = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      val name = s"kafka-consumer-${KafkaConsumerActor.Internal.nextNumber()}"
      extendedActorSystem.systemActorOf(KafkaConsumerActor.props(settings), name)
    }

    subscription match {
      case TopicSubscription(topics) =>
        consumer ! KafkaConsumerActor.Internal.Subscribe(topics, KafkaConsumerActor.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke))
      case TopicSubscriptionPattern(topics) =>
        consumer ! KafkaConsumerActor.Internal.SubscribePattern(topics, KafkaConsumerActor.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke))
      case Assignment(topics) =>
        consumer ! KafkaConsumerActor.Internal.Assign(topics)
        tps ++= topics
      case AssignmentWithOffset(topics) =>
        consumer ! KafkaConsumerActor.Internal.AssignWithOffset(topics)
        tps ++= topics.keySet
    }

    self = getStageActor {
      case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
        requested = msg.requested != tps // might be more than one in flight when we assign/revoke tps
        // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
        if (buffer.hasNext) {
          buffer = buffer ++ msg.messages
        }
        else {
          buffer = msg.messages
        }
        pump()
      case (_, Terminated(ref)) if ref == consumer =>
        failStage(new Exception("Consumer actor terminated"))
    }
    self.watch(consumer)
  }

  val partitionAssignedCB = getAsyncCallback[Iterable[TopicPartition]] { newTps =>
    tps ++= newTps
    requested = true
    consumer.tell(KafkaConsumerActor.Internal.RequestMessages(tps), self.ref)
  }
  val partitionRevokedCB = getAsyncCallback[Iterable[TopicPartition]] { newTps =>
    tps --= newTps
    requested = true
    consumer.tell(KafkaConsumerActor.Internal.RequestMessages(tps), self.ref)
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
        requested = true
        consumer.tell(KafkaConsumerActor.Internal.RequestMessages(tps), self.ref)
      }
    }
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
    super.postStop()
  }

  override def performShutdown() = {
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
