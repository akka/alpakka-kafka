/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.annotation.InternalApi
import akka.event.Logging
import akka.kafka.Subscriptions._
import akka.kafka._
import akka.stream.{ActorMaterializerHelper, SourceShape}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{Future, Promise}

/**
 * Internal API.
 *
 * Anonymous sub-class instances are created in [[CommittableSource]] and [[TransactionalSource]].
 */
@InternalApi private abstract class SingleSourceLogic[K, V, Msg](
    shape: SourceShape[Msg],
    settings: ConsumerSettings[K, V],
    subscription: Subscription
) extends BaseSingleSourceLogic[K, V, Msg](shape) {

  override protected def logSource: Class[_] = classOf[SingleSourceLogic[K, V, Msg]]

  private val consumerPromise = Promise[ActorRef]
  final val actorNumber = KafkaConsumerActor.Internal.nextNumber()
  final def consumerFuture: Future[ActorRef] = consumerPromise.future

  private val partitionLogLevel = if (settings.wakeupDebug) Logging.InfoLevel else Logging.DebugLevel

  final def configureSubscription(): Unit = {

    def rebalanceListener(autoSubscription: AutoSubscription): KafkaConsumerActor.ListenerCallbacks = {
      val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { assignedTps =>
        tps ++= assignedTps
        log.log(partitionLogLevel, "Assigned partitions: {}. All partitions: {}", assignedTps, tps)
        requestMessages()
      }

      val partitionRevokedCB = getAsyncCallback[Set[TopicPartition]] { revokedTps =>
        tps --= revokedTps
        log.log(partitionLogLevel, "Revoked partitions: {}. All partitions: {}", revokedTps, tps)
      }

      KafkaConsumerActor.ListenerCallbacks(
        assignedTps => {
          autoSubscription.rebalanceListener.foreach {
            _.tell(TopicPartitionsAssigned(autoSubscription, assignedTps), sourceActor.ref)
          }
          if (assignedTps.nonEmpty) {
            partitionAssignedCB.invoke(assignedTps)
          }
        },
        revokedTps => {
          autoSubscription.rebalanceListener.foreach {
            _.tell(TopicPartitionsRevoked(autoSubscription, revokedTps), sourceActor.ref)
          }
          if (revokedTps.nonEmpty) {
            partitionRevokedCB.invoke(revokedTps)
          }
        }
      )
    }

    subscription match {
      case sub @ TopicSubscription(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener(sub)), sourceActor.ref)
      case sub @ TopicSubscriptionPattern(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener(sub)),
                           sourceActor.ref)
      case s: ManualSubscription => configureManualSubscription(s)
    }

  }

  final def createConsumerActor(): ActorRef = {
    val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
    val actor =
      extendedActorSystem.systemActorOf(akka.kafka.KafkaConsumerActor.props(sourceActor.ref, settings),
                                        s"kafka-consumer-$actorNumber")
    consumerPromise.success(actor)
    actor
  }

  final override def postStop(): Unit = {
    consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    super.postStop()
  }

  final def performShutdown(): Unit = {
    setKeepGoing(true)
    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    sourceActor.become {
      case (_, Terminated(ref)) if ref == consumerActor =>
        onShutdown()
        completeStage()
    }
    materializer.scheduleOnce(settings.stopTimeout, new Runnable {
      override def run(): Unit =
        consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    })
  }

}
