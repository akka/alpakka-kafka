/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.kafka.internal.KafkaConsumerActor.Internal.{ConsumerMetrics, RequestMetrics}
import akka.kafka.Subscriptions._
import akka.kafka._
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{ActorMaterializerHelper, SourceShape}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.annotation.tailrec
import scala.concurrent.Future

private[kafka] abstract class SingleSourceLogic[K, V, Msg](
    val shape: SourceShape[Msg],
    settings: ConsumerSettings[K, V],
    subscription: Subscription
) extends GraphStageLogic(shape)
    with PromiseControl
    with MessageBuilder[K, V, Msg]
    with StageLogging {

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
      extendedActorSystem.systemActorOf(akka.kafka.KafkaConsumerActor.props(settings), name)
    }

    self = getStageActor {
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
      case (_, Terminated(ref)) if ref == consumer =>
        failStage(new ConsumerFailed)
    }
    self.watch(consumer)

    val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { newTps =>
      // Is info too much here? Will be logged at startup / rebalance
      tps ++= newTps
      log.log(partitionLogLevel, "Assigned partitions: {}. All partitions: {}", newTps, tps)
      notifyUserOnAssign(newTps)
      requestMessages()
    }

    val partitionRevokedCB = getAsyncCallback[Set[TopicPartition]] { newTps =>
      tps --= newTps
      log.log(partitionLogLevel, "Revoked partitions: {}. All partitions: {}", newTps, tps)
      notifyUserOnRevoke(newTps)
    }

    def rebalanceListener: KafkaConsumerActor.ListenerCallbacks = {
      val onAssign: Set[TopicPartition] ⇒ Unit = tps ⇒ partitionAssignedCB.invoke(tps)
      val onRevoke: Set[TopicPartition] ⇒ Unit = set ⇒ partitionRevokedCB.invoke(set)
      KafkaConsumerActor.rebalanceListener(onAssign, onRevoke)
    }

    subscription match {
      case TopicSubscription(topics, _) =>
        consumer.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener), self.ref)
      case TopicSubscriptionPattern(topics, _) =>
        consumer.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener), self.ref)
      case Assignment(topics, _) =>
        consumer.tell(KafkaConsumerActor.Internal.Assign(topics), self.ref)
        tps ++= topics
      case AssignmentWithOffset(topics, _) =>
        consumer.tell(KafkaConsumerActor.Internal.AssignWithOffset(topics), self.ref)
        tps ++= topics.keySet
      case AssignmentOffsetsForTimes(topics, _) =>
        consumer.tell(KafkaConsumerActor.Internal.AssignOffsetsForTimes(topics), self.ref)
        tps ++= topics.keySet
    }

  }

  private def notifyUserOnAssign(set: Set[TopicPartition]): Unit =
    subscription.rebalanceListener.foreach(ref ⇒ ref ! TopicPartitionsAssigned(subscription, set))
  private def notifyUserOnRevoke(set: Set[TopicPartition]): Unit =
    subscription.rebalanceListener.foreach(ref ⇒ ref ! TopicPartitionsRevoked(subscription, set))

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
    consumer.tell(KafkaConsumerActor.Internal.RequestMessages(requestId, tps), self.ref)
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit =
      pump()

    override def onDownstreamFinish(): Unit =
      performShutdown()
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

  override def metrics: Future[Map[MetricName, Metric]] = {
    import akka.pattern.ask
    import scala.concurrent.duration._
    consumer
      .?(RequestMetrics)(Timeout(1.minute))
      .mapTo[ConsumerMetrics]
      .map(_.metrics)(ExecutionContexts.sameThreadExecutionContext)
  }

}
