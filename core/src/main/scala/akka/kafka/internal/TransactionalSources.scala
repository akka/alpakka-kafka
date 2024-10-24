/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.Locale
import akka.{Done, NotUsed}
import akka.actor.{ActorRef, Status, Terminated}
import akka.actor.Status.Failure
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.{PartitionOffset, TransactionalMessage}
import akka.kafka.internal.KafkaConsumerActor.Internal.Revoked
import akka.kafka.internal.SubSourceLogic._
import akka.kafka.internal.TransactionalSubSourceStageLogic.DrainingComplete
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.PartitionAssignmentHandler
import akka.kafka.{AutoSubscription, ConsumerFailed, ConsumerSettings, RestrictedConsumer, Subscription}
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerGroupMetadata, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}

/** Internal API */
@InternalApi
private[kafka] final class TransactionalSource[K, V](consumerSettings: ConsumerSettings[K, V],
                                                     val subscription: Subscription)
    extends KafkaSourceStage[K, V, TransactionalMessage[K, V]](
      s"TransactionalSource ${subscription.renderStageAttribute}"
    ) {

  require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty, "You must define a Consumer group.id.")

  override protected def logic(shape: SourceShape[TransactionalMessage[K, V]]): GraphStageLogic with Control =
    new TransactionalSourceLogic(shape, TransactionalSource.txConsumerSettings(consumerSettings), subscription)
      with TransactionalMessageBuilder[K, V] {
      override val fromPartitionedSource: Boolean = false
    }
}

/** Internal API */
@InternalApi
private[internal] object TransactionalSource {

  /**
   * We set the isolation.level config to read_committed to make sure that any consumed messages are from
   * committed transactions. Note that the consuming partitions may be produced by multiple producers, and these
   * producers may either use transactional messaging or not at all. So the fetching partitions may have both
   * transactional and non-transactional messages, and by setting isolation.level config to read_committed consumers
   * will still consume non-transactional messages.
   */
  def txConsumerSettings[K, V](consumerSettings: ConsumerSettings[K, V]) = consumerSettings.withProperty(
    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
    IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH)
  )

}

/** Internal API */
@InternalApi
private[kafka] final class TransactionalSourceWithOffsetContext[K, V](consumerSettings: ConsumerSettings[K, V],
                                                                      subscription: Subscription)
    extends KafkaSourceStage[K, V, (ConsumerRecord[K, V], PartitionOffset)](
      s"TransactionalSourceWithOffsetContext ${subscription.renderStageAttribute}"
    ) {

  require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty, "You must define a Consumer group.id.")

  override protected def logic(
      shape: SourceShape[(ConsumerRecord[K, V], PartitionOffset)]
  ): GraphStageLogic with Control =
    new TransactionalSourceLogic(shape, TransactionalSource.txConsumerSettings(consumerSettings), subscription)
      with TransactionalOffsetContextBuilder[K, V] {
      override val fromPartitionedSource: Boolean = false
    }
}

/** Internal API */
@InternalApi
private[internal] abstract class TransactionalSourceLogic[K, V, Msg](shape: SourceShape[Msg],
                                                                     consumerSettings: ConsumerSettings[K, V],
                                                                     subscription: Subscription)
    extends SingleSourceLogic[K, V, Msg](shape, consumerSettings, subscription)
    with TransactionalMessageBuilderBase[K, V, Msg] {

  import TransactionalSourceLogic._

  override protected def logSource: Class[_] = classOf[TransactionalSourceLogic[_, _, _]]

  private val inFlightRecords = InFlightRecords.empty
  @volatile protected var threadSafeSourceActor: ActorRef = _
  @volatile protected var threadSafeConsumerActor: ActorRef = _

  // called in-stage (no need for volatile)
  override def consumerActorRef(): ActorRef = this.consumerActor

  override def preStart(): Unit = {
    super.preStart()
    // actually the stage actor of this stage, initialized in super preStart
    threadSafeSourceActor = this.sourceActor.ref
    threadSafeConsumerActor = this.consumerActor
  }

  override def messageHandling = super.messageHandling.orElse(drainHandling).orElse {
    case (_, Revoked(tps)) =>
      inFlightRecords.revoke(tps.toSet)
  }

  override def shuttingDownReceive =
    super.shuttingDownReceive
      .orElse(drainHandling)
      .orElse {
        case (_, Status.Failure(e)) =>
          failStage(e)
        case (_, Terminated(ref)) if ref == consumerActor =>
          failStage(new ConsumerFailed())
      }

  private def drainHandling: PartialFunction[(ActorRef, Any), Unit] = {
    case (sender, Committed(offsets)) =>
      inFlightRecords.committed(offsets.iterator.map { case (k, v) => k -> (v.offset() - 1L) }.toMap)
      sender.tell(Done, sourceActor.ref)
    case (_, CommittingFailure) => {
      log.info("Committing failed, resetting in flight offsets")
      inFlightRecords.reset()
    }
    case (sender, Drain(partitions, ack, msg)) =>
      if (inFlightRecords.empty(partitions)) {
        log.debug(s"Partitions drained ${partitions.mkString(",")}")
        ack.getOrElse(sender).tell(msg, sourceActor.ref)
      } else {
        log.debug(s"Draining partitions {}", partitions)
        materializer.scheduleOnce(
          consumerSettings.drainingCheckInterval,
          () => sourceActor.ref.tell(Drain(partitions, ack.orElse(Some(sender)), msg), sourceActor.ref)
        )
      }
  }

  override val groupId: String = consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)

  override lazy val committedMarker: CommittedMarker =
    CommittedMarkerRef(sourceActor.ref, consumerSettings.commitTimeout)

  override def onMessage(rec: ConsumerRecord[K, V]): Unit =
    inFlightRecords.add(Map(new TopicPartition(rec.topic(), rec.partition()) -> rec.offset()))

  override protected def stopConsumerActor(): Unit =
    sourceActor.ref
      .tell(Drain(
              inFlightRecords.assigned(),
              Some(consumerActor),
              KafkaConsumerActor.Internal.StopFromStage(id)
            ),
            sourceActor.ref)

  override protected def addToPartitionAssignmentHandler(
      handler: PartitionAssignmentHandler
  ): PartitionAssignmentHandler = {
    // Note: this runs on a different thread so be careful with any mutable state
    val blockingRevokedCall = new PartitionAssignmentHandler {
      override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

      // This is invoked in the KafkaConsumerActor thread when doing poll.
      override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
        if (waitForDraining(revokedTps)) {
          threadSafeSourceActor.tell(Revoked(revokedTps.toList), consumerActor)
        } else {
          threadSafeSourceActor.tell(Failure(new Error("Timeout while draining")), consumerActor)
          threadSafeConsumerActor.tell(KafkaConsumerActor.Internal.StopFromStage(id), consumerActor)
        }

      override def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
        onRevoke(lostTps, consumer)

      override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

      private def waitForDraining(partitions: Set[TopicPartition]): Boolean = {
        import akka.pattern.ask
        implicit val timeout = Timeout(consumerSettings.commitTimeout)
        try {
          Await.result(ask(threadSafeSourceActor, Drain(partitions, None, Drained)), timeout.duration)
          true
        } catch {
          case t: Throwable =>
            false
        }
      }
    }
    new PartitionAssignmentHelpers.Chain(handler, blockingRevokedCall)
  }
}

/** Internal API */
@InternalApi
private[kafka] final class TransactionalSubSource[K, V](
    consumerSettings: ConsumerSettings[K, V],
    subscription: AutoSubscription
) extends KafkaSourceStage[K, V, (TopicPartition, Source[TransactionalMessage[K, V], NotUsed])](
      s"TransactionalSubSource ${subscription.renderStageAttribute}"
    ) {
  import TransactionalSourceLogic._

  require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty, "You must define a Consumer group.id.")

  /**
   * We set the isolation.level config to read_committed to make sure that any consumed messages are from
   * committed transactions. Note that the consuming partitions may be produced by multiple producers, and these
   * producers may either use transactional messaging or not at all. So the fetching partitions may have both
   * transactional and non-transactional messages, and by setting isolation.level config to read_committed consumers
   * will still consume non-transactional messages.
   */
  private val txConsumerSettings = consumerSettings.withProperty(
    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
    IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH)
  )

  override protected def logic(
      shape: SourceShape[(TopicPartition, Source[TransactionalMessage[K, V], NotUsed])]
  ): GraphStageLogic with Control = {
    val factory = new SubSourceStageLogicFactory[K, V, TransactionalMessage[K, V]] {
      def create(
          shape: SourceShape[TransactionalMessage[K, V]],
          tp: TopicPartition,
          consumerActor: ActorRef,
          subSourceStartedCb: AsyncCallback[SubSourceStageLogicControl],
          subSourceCancelledCb: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)],
          actorNumber: Int
      ): SubSourceStageLogic[K, V, TransactionalMessage[K, V]] =
        new TransactionalSubSourceStageLogic(shape,
                                             tp,
                                             consumerActor,
                                             subSourceStartedCb,
                                             subSourceCancelledCb,
                                             actorNumber,
                                             txConsumerSettings)
    }

    new SubSourceLogic(shape, txConsumerSettings, subscription, subSourceStageLogicFactory = factory) {

      @volatile var threadSafeSourceActor: ActorRef = _
      @volatile var threadSafeConsumerActor: ActorRef = _

      override def preStart(): Unit = {
        super.preStart()
        threadSafeSourceActor = this.sourceActor.ref
        threadSafeConsumerActor = this.consumerActor
      }

      override protected def addToPartitionAssignmentHandler(
          handler: PartitionAssignmentHandler
      ): PartitionAssignmentHandler = {
        // Note: this runs on a different thread so be careful with any mutable state
        val blockingRevokedCall = new PartitionAssignmentHandler {
          override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

          // This is invoked in the KafkaConsumerActor thread when doing poll.
          override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
            if (revokedTps.isEmpty) ()
            else if (waitForDraining(revokedTps)) {
              subSources.values
                .map(_.controlAndStageActor.stageActor)
                .foreach(_.tell(Revoked(revokedTps.toList), threadSafeSourceActor))
            } else {
              threadSafeSourceActor.tell(Status.Failure(new Error("Timeout while draining")), threadSafeSourceActor)
              threadSafeConsumerActor.tell(KafkaConsumerActor.Internal.StopFromStage(id), threadSafeSourceActor)
            }

          override def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
            onRevoke(lostTps, consumer)

          override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()
        }
        new PartitionAssignmentHelpers.Chain(handler, blockingRevokedCall)
      }

      private def waitForDraining(partitions: Set[TopicPartition]): Boolean = {
        import akka.pattern.ask
        implicit val timeout = Timeout(txConsumerSettings.commitTimeout)
        try {
          val drainCommandFutures =
            subSources.values.map(_.stageActor).map(ask(_, Drain(partitions, None, Drained)))
          implicit val ec = executionContext
          Await.result(Future.sequence(drainCommandFutures), timeout.duration)
          true
        } catch {
          case _: Throwable =>
            false
        }
      }
    }
  }
}

/** Internal API */
@InternalApi
private object TransactionalSourceLogic {
  type Offset = Long

  case object Drained
  final case class Drain[T](partitions: Set[TopicPartition],
                            drainedConfirmationRef: Option[ActorRef],
                            drainedConfirmationMsg: T)
  final case class Committed(offsets: Map[TopicPartition, OffsetAndMetadata])
  case object CommittingFailure

  private[internal] final case class CommittedMarkerRef(sourceActor: ActorRef, commitTimeout: FiniteDuration)
      extends CommittedMarker {
    override def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Done] = {
      import akka.pattern.ask
      sourceActor
        .ask(Committed(offsets))(Timeout(commitTimeout))
        .map(_ => Done)(ExecutionContext.parasitic)
    }

    override def failed(): Unit =
      sourceActor ! CommittingFailure
  }

  private[internal] trait InFlightRecords {
    // Assumes that offsets per topic partition are added in the increasing order
    // The assumption is true for Kafka consumer that guarantees that elements are emitted
    // per partition in offset-increasing order.
    def add(offsets: Map[TopicPartition, Offset]): Unit
    def committed(offsets: Map[TopicPartition, Offset]): Unit
    def revoke(revokedTps: Set[TopicPartition]): Unit
    def reset(): Unit
    def assigned(): Set[TopicPartition]

    def empty(partitions: Set[TopicPartition]): Boolean
  }

  private[internal] object InFlightRecords {
    def empty = new Impl

    class Impl extends InFlightRecords {
      private var inFlightRecords: Map[TopicPartition, Offset] = Map.empty

      override def add(offsets: Map[TopicPartition, Offset]): Unit =
        inFlightRecords = inFlightRecords ++ offsets

      override def committed(committed: Map[TopicPartition, Offset]): Unit =
        inFlightRecords = inFlightRecords.flatMap {
          case (tp, offset) if committed.get(tp).contains(offset) => None
          case x => Some(x)
        }

      override def revoke(revokedTps: Set[TopicPartition]): Unit =
        inFlightRecords = inFlightRecords -- revokedTps

      override def reset(): Unit = inFlightRecords = Map.empty

      override def empty(partitions: Set[TopicPartition]): Boolean = partitions.flatMap(inFlightRecords.get(_)).isEmpty

      override def toString: String = inFlightRecords.toString()

      override def assigned(): Set[TopicPartition] = inFlightRecords.keySet
    }
  }
}

@InternalApi
private final class TransactionalSubSourceStageLogic[K, V](
    shape: SourceShape[TransactionalMessage[K, V]],
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[SubSourceStageLogicControl],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)],
    actorNumber: Int,
    consumerSettings: ConsumerSettings[K, V]
) extends SubSourceStageLogic[K, V, TransactionalMessage[K, V]](shape,
                                                                  tp,
                                                                  consumerActor,
                                                                  subSourceStartedCb,
                                                                  subSourceCancelledCb,
                                                                  actorNumber)
    with TransactionalMessageBuilder[K, V] {

  import TransactionalSourceLogic._

  private val inFlightRecords = InFlightRecords.empty

  override lazy val committedMarker: CommittedMarker =
    CommittedMarkerRef(subSourceActor.ref, consumerSettings.commitTimeout)

  override val fromPartitionedSource: Boolean = true
  override def groupId: String = consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)

  override def onMessage(rec: ConsumerRecord[K, V]): Unit =
    inFlightRecords.add(Map(new TopicPartition(rec.topic(), rec.partition()) -> rec.offset()))

  override protected def messageHandling: PartialFunction[(ActorRef, Any), Unit] =
    super.messageHandling.orElse(drainHandling).orElse {
      case (_, Revoked(tps)) =>
        inFlightRecords.revoke(tps.toSet)
    }

  override protected def onDownstreamFinishSubSourceCancellationStrategy(): SubSourceCancellationStrategy = DoNothing

  private def shuttingDownReceive: PartialFunction[(ActorRef, Any), Unit] =
    drainHandling
      .orElse {
        case (_, Status.Failure(e)) =>
          failStage(e)
        case (_, Terminated(ref)) if ref == consumerActor =>
          failStage(new ConsumerFailed())
      }

  override def performShutdown(): Unit = {
    log.debug("#{} Completing SubSource for partition {}", actorNumber, tp)
    setKeepGoing(true)
    if (!isClosed(shape.out)) {
      complete(shape.out) // initiate shutdown of SubSource
    }
    subSourceActor.become(shuttingDownReceive)
    drainAndComplete()
  }

  private def drainAndComplete(): Unit =
    subSourceActor.ref.tell(Drain(inFlightRecords.assigned(), None, DrainingComplete), subSourceActor.ref)

  private def drainHandling: PartialFunction[(ActorRef, Any), Unit] = {
    case (sender, Committed(offsets)) =>
      inFlightRecords.committed(offsets.iterator.map { case (k, v) => k -> (v.offset() - 1L) }.toMap)
      sender ! Done
    case (sender, CommittingFailure) => {
      log.info("Committing failed, resetting in flight offsets")
      inFlightRecords.reset()
    }
    case (sender, Drain(partitions, ack, msg)) =>
      if (inFlightRecords.empty(partitions)) {
        log.debug(s"Partitions drained ${partitions.mkString(",")}")
        ack.getOrElse(sender) ! msg
      } else {
        log.debug(s"Draining partitions {}", partitions)
        materializer.scheduleOnce(
          consumerSettings.drainingCheckInterval,
          () => subSourceActor.ref.tell(Drain(partitions, ack.orElse(Some(sender)), msg), stageActor.ref)
        )
      }
    case (sender, DrainingComplete) =>
      completeStage()
  }

  override def consumerActorRef(): ActorRef = consumerActor
}

private object TransactionalSubSourceStageLogic {
  case object DrainingComplete
}
