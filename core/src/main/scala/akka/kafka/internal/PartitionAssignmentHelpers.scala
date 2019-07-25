/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.kafka.scaladsl.PartitionAssignmentHandler
import akka.kafka.{AutoSubscription, RestrictedConsumer, TopicPartitionsAssigned, TopicPartitionsRevoked}
import akka.stream.stage.AsyncCallback
import org.apache.kafka.common.TopicPartition

/**
 * Internal API.
 *
 * Implementations of [[PartitionAssignmentHandler]] for internal use.
 */
@InternalApi
object PartitionAssignmentHelpers {

  @InternalApi
  object EmptyPartitionAssignmentHandler extends PartitionAssignmentHandler {
    override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

    override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

    override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()
  }

  @InternalApi
  final class AsyncCallbacks(subscription: AutoSubscription,
                             sourceActor: ActorRef,
                             partitionAssignedCB: AsyncCallback[Set[TopicPartition]],
                             partitionRevokedCB: AsyncCallback[Set[TopicPartition]])
      extends PartitionAssignmentHandler {

    override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      subscription.rebalanceListener.foreach {
        _.tell(TopicPartitionsRevoked(subscription, revokedTps), sourceActor)
      }
      if (revokedTps.nonEmpty) {
        partitionRevokedCB.invoke(revokedTps)
      }
    }

    override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      subscription.rebalanceListener.foreach {
        _.tell(TopicPartitionsAssigned(subscription, assignedTps), sourceActor)
      }
      if (assignedTps.nonEmpty) {
        partitionAssignedCB.invoke(assignedTps)
      }
    }

    override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()
  }

  @InternalApi
  final class Chain(handler1: PartitionAssignmentHandler, handler2: PartitionAssignmentHandler)
      extends PartitionAssignmentHandler {
    override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      handler1.onRevoke(revokedTps, consumer)
      handler2.onRevoke(revokedTps, consumer)
    }

    override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      handler1.onAssign(assignedTps, consumer)
      handler2.onAssign(assignedTps, consumer)
    }

    override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      handler1.onStop(revokedTps, consumer)
      handler2.onStop(revokedTps, consumer)
    }
  }

}
