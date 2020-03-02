/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal
import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.kafka.{AutoSubscription, ManualSubscription, Subscription}
import akka.kafka.Subscriptions._
import akka.kafka.scaladsl.PartitionAssignmentHandler
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
import org.apache.kafka.common.TopicPartition

/**
 * Common subscription logic that's shared across sources.
 *
 * The implementation can inject its own behaviour in two ways:
 *
 * 1. Asynchronously by providing [[AsyncCallback]]s for rebalance events
 * 2. Synchronously by overriding `addToPartitionAssignmentHandler`
 */
@InternalApi
private[kafka] trait SourceLogicSubscription {
  self: GraphStageLogic =>

  protected def subscription: Subscription

  protected def consumerActor: ActorRef
  protected def sourceActor: StageActor

  protected def configureSubscription(partitionAssignedCB: AsyncCallback[Set[TopicPartition]],
                                      partitionRevokedCB: AsyncCallback[Set[TopicPartition]]): Unit = {

    def rebalanceListener(autoSubscription: AutoSubscription): PartitionAssignmentHandler = {
      PartitionAssignmentHelpers.chain(
        addToPartitionAssignmentHandler(autoSubscription.partitionAssignmentHandler),
        new PartitionAssignmentHelpers.AsyncCallbacks(autoSubscription,
                                                      sourceActor.ref,
                                                      partitionAssignedCB,
                                                      partitionRevokedCB)
      )
    }

    subscription match {
      case sub @ TopicSubscription(topics, _, _) =>
        consumerActor.tell(
          KafkaConsumerActor.Internal.Subscribe(
            topics,
            addToPartitionAssignmentHandler(rebalanceListener(sub))
          ),
          sourceActor.ref
        )
      case sub @ TopicSubscriptionPattern(topics, _, _) =>
        consumerActor.tell(
          KafkaConsumerActor.Internal.SubscribePattern(
            topics,
            addToPartitionAssignmentHandler(rebalanceListener(sub))
          ),
          sourceActor.ref
        )
      case s: ManualSubscription => configureManualSubscription(s)
    }
  }

  protected def configureManualSubscription(subscription: ManualSubscription): Unit = ()

  /**
   * Opportunity for subclasses to add a different logic to the partition assignment callbacks.
   */
  protected def addToPartitionAssignmentHandler(handler: PartitionAssignmentHandler): PartitionAssignmentHandler =
    handler
}
