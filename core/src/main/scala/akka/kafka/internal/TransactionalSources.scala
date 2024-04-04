/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.Locale
import akka.{Done, NotUsed}
import akka.actor.{ActorRef, Status, Terminated}
import akka.actor.Status.Failure
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.pattern.ask
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

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise, TimeoutException}
import scala.util.{Success, Try}

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
  private val onRevokeCB = getAsyncCallback[Revoke](onRevoke).invoke _
  private val onRevokeDrainDoneCB = getAsyncCallback[(Revoke, Try[RevokeDrainDone.type])](onRevokeDrainDone).invoke _

  override def messageHandling: PartialFunction[(ActorRef, Any), Unit] =
    super.messageHandling.orElse(drainHandling).orElse {
      case (_, Revoked(tps)) =>
        inFlightRecords.revoke(tps.toSet)
    }

  override def shuttingDownReceive: PartialFunction[(ActorRef, Any), Unit] =
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
      inFlightRecords.committed(offsets.view.mapValues(v => v.offset() - 1L).toMap)
      sender.tell(Done, sourceActor.ref)
    case (_, CommittingFailure) =>
      log.info("Committing failed, resetting in flight offsets")
      inFlightRecords.reset()
    case (sender, Drain(partitions, ack, msg)) =>
      if (inFlightRecords.empty(partitions)) {
        if (log.isDebugEnabled)
          log.debug(s"Partitions drained [{}]", partitions.mkString(","))
        ack.getOrElse(sender).tell(msg, sourceActor.ref)
      } else {
        if (log.isDebugEnabled)
          log.debug(s"Draining partitions [{}]", partitions.mkString(", "))
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
  ): PartitionAssignmentHandler =
    new PartitionAssignmentHelpers.Chain(handler,
                                         createBlockingPartitionAssignmentHandler(consumerSettings, onRevokeCB))

  def onRevoke(revoke: Revoke): Unit = {
    // Tricky chain of async interactions - draining is a timed async wait and both steps
    // needs to interact with stage internal mutable state, and finally complete or fail a promise
    // whose future the blocking partition assignment handler blocks the consumer on.
    // Simplifying is tricky since other logic depends on message-send-drain
    if (log.isDebugEnabled)
      log.debug("onRevoke [{}]", revoke.partitions.mkString(","))
    stageActor.ref
      .ask(Drain(revoke.partitions, None, Drained))(consumerSettings.commitTimeout)
      .transform(tryDrain => Success((revoke, tryDrain.map(_ => RevokeDrainDone))))(ExecutionContexts.parasitic)
      .foreach(onRevokeDrainDoneCB)(ExecutionContexts.parasitic)
  }

  def onRevokeDrainDone(revokeDrainDone: (Revoke, Try[RevokeDrainDone.type])): Unit = {
    revokeDrainDone match {
      case (revoke, scala.util.Success(RevokeDrainDone)) =>
        if (log.isDebugEnabled)
          log.debug("onRevokeDrainDone [{}]", revoke.partitions.mkString(","))
        inFlightRecords.revoke(revoke.partitions)
        revoke.revokeCompletion.success(Done)
      case (revoke, scala.util.Failure(ex)) =>
        if (log.isDebugEnabled)
          log.debug("onRevokeDrainDone, drain failed [{}] ({})", revoke.partitions.mkString(","), ex)
        stageActor.ref.tell(Failure(new TimeoutException(s"Timeout while draining ($ex)")), consumerActor)
        consumerActor.tell(KafkaConsumerActor.Internal.StopFromStage(id), consumerActor)
        // we don't signal failure back, just completion
        revoke.revokeCompletion.success(Done)
    }
  }

  def requestConsumerGroupMetadata(): Future[ConsumerGroupMetadata] = {
    // use some sensible existing timeout setting for this consumer actor ask
    implicit val timeout: Timeout = consumerSettings.metadataRequestTimeout
    consumerActor
      .ask(KafkaConsumerActor.Internal.GetConsumerGroupMetadata)
      .mapTo[ConsumerGroupMetadata]
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

      private val onRevokeCB = getAsyncCallback[Revoke](onRevoke).invoke _
      private val onRevokeDrainDoneCB =
        getAsyncCallback[(Revoke, Try[RevokeDrainDone.type])](onRevokeDrainDone).invoke _

      override protected def addToPartitionAssignmentHandler(
          handler: PartitionAssignmentHandler
      ): PartitionAssignmentHandler =
        new PartitionAssignmentHelpers.Chain(
          handler,
          TransactionalSourceLogic.createBlockingPartitionAssignmentHandler(consumerSettings, onRevokeCB)
        )

      def onRevoke(revoke: Revoke): Unit = {
        // Tricky chain of async interactions - draining is a timed async wait and both steps
        // needs to interact with stage internal mutable state, and finally complete or fail a promise
        // whose future the blocking partition assignment handler blocks the consumer on.
        // Simplifying is tricky since other logic depends on message-send-drain
        implicit val timeout: Timeout = Timeout(txConsumerSettings.commitTimeout)
        implicit val ec: ExecutionContext = materializer.executionContext
        if (log.isDebugEnabled)
          log.debug("onRevoke [{}]", revoke.partitions.mkString(","))
        val drainCommandFutures =
          Future.sequence(subSources.values.map(_.stageActor.ask(Drain(revoke.partitions, None, Drained))))
        drainCommandFutures
          .transform(tryDrainAll => Success((revoke, tryDrainAll.map(_ => RevokeDrainDone))))(
            ExecutionContexts.parasitic
          )
          .foreach(onRevokeDrainDoneCB)
      }

      def onRevokeDrainDone(revokeDrainDone: (Revoke, Try[RevokeDrainDone.type])): Unit = {
        revokeDrainDone match {
          case (revoke, scala.util.Success(RevokeDrainDone)) =>
            if (log.isDebugEnabled)
              log.debug("onRevokeDrainDone [{}]", revoke.partitions.mkString(","))
            subSources.values
              .map(_.controlAndStageActor.stageActor)
              .foreach(_.tell(Revoked(revoke.partitions.toList), stageActor.ref))
            revoke.revokeCompletion.success(Done)
          case (revoke, scala.util.Failure(ex)) =>
            if (log.isDebugEnabled)
              log.debug("onRevokeDrainDone, drain failed [{}] ({})", revoke.partitions.mkString(","), ex)
            sourceActor.ref.tell(Status.Failure(new TimeoutException("Timeout while draining")), stageActor.ref)
            consumerActor.tell(KafkaConsumerActor.Internal.StopFromStage(id), stageActor.ref)
            // we don't signal failure back, just completion
            revoke.revokeCompletion.success(Done)
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

  final case class Revoke(partitions: Set[TopicPartition], revokeCompletion: Promise[Done])
  case object RevokeDrainDone

  private[internal] final case class CommittedMarkerRef(sourceActor: ActorRef, commitTimeout: FiniteDuration)
      extends CommittedMarker {
    override def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Done] = {
      sourceActor
        .ask(Committed(offsets))(Timeout(commitTimeout))
        .map(_ => Done)(ExecutionContexts.parasitic)
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

  private[internal] def createBlockingPartitionAssignmentHandler(
      consumerSettings: ConsumerSettings[_, _],
      revokeCallback: Revoke => Unit
  ): PartitionAssignmentHandler =
    new PartitionAssignmentHandler {
      override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()

      // This is invoked in the KafkaConsumerActor thread when doing poll.
      override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
        if (revokedTps.nonEmpty) {
          val revokeDone = Promise[Done]()
          revokeCallback(Revoke(revokedTps, revokeDone))
          Await.result(revokeDone.future, consumerSettings.commitTimeout * 2)
        }
      }

      override def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
        onRevoke(lostTps, consumer)

      override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()
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
      inFlightRecords.committed(offsets.view.mapValues(v => v.offset() - 1L).toMap)
      sender ! Done
    case (_, CommittingFailure) =>
      log.info("Committing failed, resetting in flight offsets")
      inFlightRecords.reset()

    case (sender, Drain(partitions, ack, msg)) =>
      if (inFlightRecords.empty(partitions)) {
        if (log.isDebugEnabled)
          log.debug(s"Partitions drained [{}]", partitions.mkString(","))
        ack.getOrElse(sender) ! msg
      } else {
        if (log.isDebugEnabled)
          log.debug(s"Draining partitions [{}]", partitions.mkString(","))
        materializer.scheduleOnce(
          consumerSettings.drainingCheckInterval,
          () => subSourceActor.ref.tell(Drain(partitions, ack.orElse(Some(sender)), msg), stageActor.ref)
        )
      }
    case (_, DrainingComplete) =>
      completeStage()
  }

  def requestConsumerGroupMetadata(): Future[ConsumerGroupMetadata] = {
    // use some sensible existing timeout setting for this consumer actor ask
    implicit val timeout: Timeout = consumerSettings.metadataRequestTimeout
    consumerActor
      .ask(KafkaConsumerActor.Internal.GetConsumerGroupMetadata)
      .mapTo[ConsumerGroupMetadata]
  }

}

private object TransactionalSubSourceStageLogic {
  case object DrainingComplete
}
