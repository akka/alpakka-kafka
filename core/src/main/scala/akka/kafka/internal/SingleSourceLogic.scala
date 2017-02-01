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
import scala.collection.mutable
import scala.annotation.tailrec

private[kafka] abstract class SingleSourceLogic[K, V, Msg](
    val shape: SourceShape[Msg],
    settings: ConsumerSettings[K, V],
    subscription: Subscription
) extends GraphStageLogic(shape) with PromiseControl with MessageBuilder[K, V, Msg] {
  val partitionAssignedCB = getAsyncCallback[Iterable[TopicPartition]] { newTps =>
    tps ++= newTps
    requestMessage()
  }
  val partitionRevokedCB = getAsyncCallback[Iterable[TopicPartition]] { newTps =>
    tps --= newTps
    requestMessage()
  }
  var consumer: ActorRef = _
  var self: StageActor = _
  var tps = Set.empty[TopicPartition]
  var requestId = 0
  var shutdownStarted = false

  val bufferSize = settings.sourceBufferSize
  val buffer: mutable.Queue[ConsumerRecord[K, V]] = mutable.Queue()
  var paused = false
  val pauseMessage = KafkaConsumerActor.Internal.PauseMessages(0, tps)
  val unpauseMessage = KafkaConsumerActor.Internal.UnpauseMessages(0, tps)

  def checkBufferAndPause(): Unit =
    if (!paused && buffer.size >= bufferSize) {
      consumer.tell(pauseMessage, self.ref)
      paused = true
    }

  def checkBufferAndUnpause(): Unit =
    if (paused && buffer.size < (bufferSize * 0.5)) {
      consumer.tell(unpauseMessage, self.ref)
      paused = false
    }

  override def preStart(): Unit = {
    super.preStart()

    consumer = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      val name = s"kafka-consumer-${KafkaConsumerActor.Internal.nextNumber()}"
      extendedActorSystem.systemActorOf(KafkaConsumerActor.props(settings), name)
    }

    self = getStageActor {
      case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
        buffer.enqueue(msg.messages.toSeq: _*)
        checkBufferAndPause()
        pump()
      case (_, Terminated(ref)) if ref == consumer =>
        failStage(new Exception("Consumer actor terminated"))
    }
    self.watch(consumer)

    def rebalanceListener =
      KafkaConsumerActor.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke)

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
    }

  }

  private def requestMessage(): Unit = {
    consumer.tell(KafkaConsumerActor.Internal.RequestMessages(requestId, tps), self.ref)
  }

  override def postStop(): Unit = {
    consumer ! KafkaConsumerActor.Internal.Stop
    onShutdown()
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

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = {
      pump()
    }

    override def onDownstreamFinish(): Unit = {
      performShutdown()
    }
  })

  @tailrec
  private def pump(): Unit = {
    if (isAvailable(shape.out)) {
      if (buffer.nonEmpty) {
        val msg = buffer.dequeue()
        push(shape.out, createMessage(msg))
        checkBufferAndUnpause()
        pump()
      }
    }
  }
}
