/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.NotUsed
import akka.actor.Status
import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.annotation.InternalApi
import akka.kafka.internal.KafkaConsumerActor.Internal.RegisterSubStage
import akka.kafka.internal.SubSourceLogic._
import akka.kafka.{AutoSubscription, ConsumerFailed, ConsumerSettings, RestrictedConsumer}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.PartitionAssignmentHandler
import akka.pattern.{ask, AskTimeoutException}
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Internal API.
 *
 * A `SubSourceLogic` is used to create partitioned sources from the various types of sub sources available:
 * [[CommittableSubSource]], [[PlainSubSource]] and [[TransactionalSubSource]].
 *
 * A `SubSourceLogic` emits a source of `SubSourceStage` per subscribed Kafka partition.
 *
 * The `SubSourceLogic.subSourceStageLogicFactory` parameter is passed to each `SubSourceStage` so that a new
 * `SubSourceStageLogic` can be created for each stage. Context parameters from the `SubSourceLogic` are passed down to
 * `SubSourceStage` and on to the `SubSourceStageLogicFactory` when the stage creates a `GraphStageLogic`.
 *
 */
@InternalApi
private class SubSourceLogic[K, V, Msg](
    val shape: SourceShape[(TopicPartition, Source[Msg, NotUsed])],
    settings: ConsumerSettings[K, V],
    override protected val subscription: AutoSubscription,
    getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]] = None,
    onRevoke: Set[TopicPartition] => Unit = _ => (),
    subSourceStageLogicFactory: SubSourceStageLogicFactory[K, V, Msg]
) extends TimerGraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with SourceLogicSubscription
    with StageIdLogging {
  import SubSourceLogic._

  private val consumerPromise = Promise[ActorRef]()
  final val actorNumber = KafkaConsumerActor.Internal.nextNumber()
  override def id: String = s"${super.id}#$actorNumber"
  override def executionContext: ExecutionContext = materializer.executionContext
  override def consumerFuture: Future[ActorRef] = consumerPromise.future

  protected var consumerActor: ActorRef = _
  protected var sourceActor: StageActor = _

  /** Kafka has notified us that we have these partitions assigned, but we have not created a source for them yet. */
  private var pendingPartitions: immutable.Set[TopicPartition] = immutable.Set.empty

  /** We have created a source for these partitions, but it has not started up and is not in subSources yet. */
  private var partitionsInStartup: immutable.Set[TopicPartition] = immutable.Set.empty
  protected var subSources: Map[TopicPartition, SubSourceStageLogicControl] = immutable.Map.empty

  /** Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking. */
  private var partitionsToRevoke: Set[TopicPartition] = Set.empty

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting")
    sourceActor = getStageActor {
      case (_, Status.Failure(e)) =>
        failStage(e)
      case (_, Terminated(ref)) if ref == consumerActor =>
        failStage(new ConsumerFailed)
      case (_, msg) =>
        log.warning("ignoring message [{}]", msg)
    }
    consumerActor = {
      val extendedActorSystem = materializer.system.asInstanceOf[ExtendedActorSystem]
      extendedActorSystem.systemActorOf(akka.kafka.KafkaConsumerActor.props(sourceActor.ref, settings),
                                        s"kafka-consumer-$actorNumber")
    }
    consumerPromise.success(consumerActor)
    sourceActor.watch(consumerActor)

    configureSubscription(partitionAssignedCB, partitionRevokedCB)
  }

  private val updatePendingPartitionsAndEmitSubSourcesCb =
    getAsyncCallback[Set[TopicPartition]](updatePendingPartitionsAndEmitSubSources)

  private val stageFailCB = getAsyncCallback[ConsumerFailed] { ex =>
    failStage(ex)
  }

  private val onOffsetsFromExternalResponseCB = getAsyncCallback[(Set[TopicPartition], Map[TopicPartition, Long])] {
    case (formerlyUnknown, offsetMap) =>
      val updatedFormerlyUnknown = formerlyUnknown -- (partitionsToRevoke ++ partitionsInStartup ++ pendingPartitions)
      // Filter out the offsetMap so that we don't re-seek for partitions that have been revoked
      seekAndEmitSubSources(updatedFormerlyUnknown, offsetMap.iterator.filterNot {
        case (k, _) => partitionsToRevoke.contains(k)
      }.toMap)
  }

  private val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { assigned =>
    val formerlyUnknown = assigned -- partitionsToRevoke

    if (log.isDebugEnabled && formerlyUnknown.nonEmpty) {
      log.debug("Assigning new partitions: {}", formerlyUnknown.mkString(", "))
    }

    // make sure re-assigned partitions don't get closed on CloseRevokedPartitions timer
    partitionsToRevoke = partitionsToRevoke -- assigned

    getOffsetsOnAssign match {
      case None =>
        updatePendingPartitionsAndEmitSubSources(formerlyUnknown)

      case Some(getOffsetsFromExternal) =>
        implicit val ec: ExecutionContext = materializer.executionContext
        getOffsetsFromExternal(assigned)
          .onComplete {
            case Failure(ex) =>
              stageFailCB.invoke(
                new ConsumerFailed(
                  s"$idLogPrefix Failed to fetch offset for partitions: ${formerlyUnknown.mkString(", ")}.",
                  ex
                )
              )
            case Success(offsets) =>
              onOffsetsFromExternalResponseCB.invoke((formerlyUnknown, offsets))
          }
    }
  }

  private val partitionRevokedCB = getAsyncCallback[Set[TopicPartition]] { revoked =>
    partitionsToRevoke ++= revoked
    scheduleOnce(CloseRevokedPartitions, settings.waitClosePartition)
  }

  private def seekAndEmitSubSources(
      formerlyUnknown: Set[TopicPartition],
      offsets: Map[TopicPartition, Long]
  ): Unit = {
    implicit val ec: ExecutionContext = materializer.executionContext
    consumerActor
      .ask(KafkaConsumerActor.Internal.Seek(offsets))(Timeout(10.seconds), sourceActor.ref)
      .map(_ => updatePendingPartitionsAndEmitSubSourcesCb.invoke(formerlyUnknown))
      .recover {
        case _: AskTimeoutException =>
          stageFailCB.invoke(
            new ConsumerFailed(
              s"$idLogPrefix Consumer failed during seek for partitions: ${offsets.keys.mkString(", ")}."
            )
          )
      }
  }

  override def onTimer(timerKey: Any): Unit = timerKey match {
    case CloseRevokedPartitions =>
      if (log.isDebugEnabled) {
        log.debug("Closing SubSources for revoked partitions: {}", partitionsToRevoke.mkString(", "))
      }
      onRevoke(partitionsToRevoke)
      pendingPartitions --= partitionsToRevoke
      partitionsInStartup --= partitionsToRevoke
      partitionsToRevoke.flatMap(subSources.get).map(_.control).foreach(_.shutdown())
      subSources --= partitionsToRevoke
      partitionsToRevoke = Set.empty
    case _ => log.warning("unexpected timer [{}]", timerKey)
  }

  private val subsourceCancelledCB: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)] =
    getAsyncCallback[(TopicPartition, SubSourceCancellationStrategy)] {
      case (tp, cancellationStrategy: SubSourceCancellationStrategy) =>
        subSources -= tp
        partitionsInStartup -= tp

        cancellationStrategy match {
          case SeekToOffsetAndReEmit(offset) =>
            // re-add this partition to pending partitions so it can be re-emitted
            pendingPartitions += tp
            if (log.isDebugEnabled) {
              log.debug("Seeking {} to {} after partition SubSource cancelled", tp, offset)
            }
            seekAndEmitSubSources(formerlyUnknown = Set.empty, Map(tp -> offset))
          case ReEmit =>
            // re-add this partition to pending partitions so it can be re-emitted
            pendingPartitions += tp
            emitSubSourcesForPendingPartitions()
          case DoNothing =>
        }
    }

  private val subsourceStartedCB: AsyncCallback[SubSourceStageLogicControl] =
    getAsyncCallback[SubSourceStageLogicControl] {
      case value @ SubSourceStageLogicControl(tp, ControlAndStageActor(control, _), _) =>
        if (!partitionsInStartup.contains(tp)) {
          // Partition was revoked while
          // starting up.  Kill!
          control.shutdown()
        } else {
          subSources += (tp -> value)
          partitionsInStartup -= tp
        }
    }

  setHandler(
    shape.out,
    new OutHandler {
      override def onPull(): Unit =
        emitSubSourcesForPendingPartitions()
      override def onDownstreamFinish(cause: Throwable): Unit = {
        super.onDownstreamFinish(cause)
        performShutdown(cause)
      }
    }
  )

  private def updatePendingPartitionsAndEmitSubSources(formerlyUnknownPartitions: Set[TopicPartition]): Unit = {
    pendingPartitions ++= formerlyUnknownPartitions.filter(!partitionsInStartup.contains(_))
    emitSubSourcesForPendingPartitions()
  }

  @tailrec
  private def emitSubSourcesForPendingPartitions(): Unit =
    if (pendingPartitions.nonEmpty && isAvailable(shape.out)) {
      val tp = pendingPartitions.head

      pendingPartitions = pendingPartitions.tail
      partitionsInStartup += tp
      val subSource = Source.fromGraph(
        new SubSourceStage(tp,
                           consumerActor,
                           subsourceStartedCB,
                           subsourceCancelledCB,
                           actorNumber,
                           subSourceStageLogicFactory)
      )
      push(shape.out, (tp, subSource))
      emitSubSourcesForPendingPartitions()
    }

  override def postStop(): Unit = {
    consumerActor.tell(KafkaConsumerActor.Internal.StopFromStage(id), sourceActor.ref)
    onShutdown()
    super.postStop()
  }

  override def performStop(): Unit = {
    setKeepGoing(true)
    subSources.values.foreach {
      _.control.stop()
    }
    complete(shape.out)
    onStop()
  }

  override def performShutdown(cause: Throwable): Unit = {
    log.info("Completing ({}). Partitions [{}], StageActor {}", cause, subSources.keys.mkString(","), sourceActor.ref)
    setKeepGoing(true)
    //todo we should wait for subsources to be shutdown and next shutdown main stage
    subSources.values.foreach {
      _.control.shutdown()
    }
    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    sourceActor.become {
      case (_, Terminated(ref)) if ref == consumerActor =>
        onShutdown()
        completeStage()
      case (_, msg) =>
        log.warning("ignoring message [{}]", msg)
    }
    stopConsumerActor(cause)
  }

  private def stopConsumerActor(cause: Throwable): Unit = {
    def performImmediateShutdown(): Unit =
      consumerActor.tell(KafkaConsumerActor.Internal.StopFromStage(id), sourceActor.ref)

    // TODO faster shutdown for SubscriptionWithCancelException.NonFailureCancellation
    // but ensure commits are submitted
    materializer.scheduleOnce(
      settings.stopTimeout,
      () => performImmediateShutdown()
    )
  }

  /**
   * Opportunity for subclasses to add a different logic to the partition assignment callbacks.
   */
  override protected def addToPartitionAssignmentHandler(
      handler: PartitionAssignmentHandler
  ): PartitionAssignmentHandler = {
    val flushMessagesOfRevokedPartitions: PartitionAssignmentHandler = new PartitionAssignmentHandler {
      private var lastRevoked = Set.empty[TopicPartition]

      override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
        lastRevoked = revokedTps

      override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
        for {
          tp <- lastRevoked -- assignedTps
          control <- subSources.get(tp)
        } control.filterRevokedPartitionsCB.invoke(Set(tp))

      override def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
        for {
          tp <- lostTps
          control <- subSources.get(tp)
        } control.filterRevokedPartitionsCB.invoke(Set(tp))

      override def onStop(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()
    }
    new PartitionAssignmentHelpers.Chain(handler, flushMessagesOfRevokedPartitions)
  }
}

/** Internal API */
private object SubSourceLogic {
  case object CloseRevokedPartitions

  /** Internal API
   *
   * SubSourceStageLogic [[akka.kafka.scaladsl.Consumer.Control]] and the stage actor [[ActorRef]]
   */
  @InternalApi
  final case class ControlAndStageActor(control: Control, stageActor: ActorRef)

  @InternalApi
  final case class SubSourceStageLogicControl(tp: TopicPartition,
                                              controlAndStageActor: ControlAndStageActor,
                                              filterRevokedPartitionsCB: AsyncCallback[Set[TopicPartition]]) {
    def control: Control = controlAndStageActor.control
    def stageActor: ActorRef = controlAndStageActor.stageActor
  }

  /** Internal API
   * Used to determine how the [[SubSourceLogic]] will handle the cancellation of a sub source stage.  The default
   * behavior requested by the [[SubSourceStageLogic]] is to ask the consumer to seek to the last committed offset and
   * then re-emit the sub source stage downstream.
   */
  sealed trait SubSourceCancellationStrategy
  final case class SeekToOffsetAndReEmit(offset: Long) extends SubSourceCancellationStrategy
  case object ReEmit extends SubSourceCancellationStrategy
  case object DoNothing extends SubSourceCancellationStrategy

  /** Internal API
   *
   * Encapsulates a factory method to create a [[SubSourceStageLogic]] within [[SubSourceLogic]] where the context
   * parameters exist.
   */
  @InternalApi
  trait SubSourceStageLogicFactory[K, V, Msg] {
    def create(
        shape: SourceShape[Msg],
        tp: TopicPartition,
        consumerActor: ActorRef,
        subSourceStartedCb: AsyncCallback[SubSourceStageLogicControl],
        subSourceCancelledCb: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)],
        actorNumber: Int
    ): SubSourceStageLogic[K, V, Msg]
  }
}

/** Internal API
 *
 * A [[SubSourceStage]] is created per partition in [[SubSourceLogic]].
 */
@InternalApi
private final class SubSourceStage[K, V, Msg](
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[SubSourceStageLogicControl],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)],
    actorNumber: Int,
    subSourceStageLogicFactory: SubSourceStageLogicFactory[K, V, Msg]
) extends GraphStage[SourceShape[Msg]] { stage =>

  val out = Outlet[Msg]("out")
  val shape: SourceShape[Msg] = new SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    subSourceStageLogicFactory.create(shape, tp, consumerActor, subSourceStartedCb, subSourceCancelledCb, actorNumber)
}

/** Internal API
 *
 * A [[SubSourceStageLogic]] is the [[GraphStageLogic]] of a [[SubSourceStage]].
 * This emits Kafka messages downstream (not sources).
 */
@InternalApi
private abstract class SubSourceStageLogic[K, V, Msg](
    val shape: SourceShape[Msg],
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[SubSourceStageLogicControl],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, SubSourceCancellationStrategy)],
    actorNumber: Int
) extends GraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with StageIdLogging
    with MessageBuilder[K, V, Msg]
    with SourceLogicBuffer[K, V, Msg] {

  override def executionContext: ExecutionContext = materializer.executionContext
  override def consumerFuture: Future[ActorRef] = Future.successful(consumerActor)
  override def id: String = s"${super.id}#$actorNumber"
  private val requestMessages = KafkaConsumerActor.Internal.RequestMessages(0, Set(tp))
  private var requested = false
  protected var subSourceActor: StageActor = _

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting. Partition {}", tp)
    subSourceActor = getStageActor(messageHandling)
    subSourceActor.watch(consumerActor)
    val controlAndActor = ControlAndStageActor(this.asInstanceOf[Control], subSourceActor.ref)
    val started = SubSourceStageLogicControl(tp, controlAndActor, filterRevokedPartitionsCB)
    subSourceStartedCb.invoke(started)
    consumerActor.tell(RegisterSubStage(requestMessages.tps), subSourceActor.ref)
  }

  protected def messageHandling: PartialFunction[(ActorRef, Any), Unit] = {
    case (_, msg: KafkaConsumerActor.Internal.Messages[K @unchecked, V @unchecked]) =>
      requested = false
      buffer = buffer ++ msg.messages
      pump()
    case (_, Status.Failure(e)) =>
      failStage(e)
    case (_, Terminated(ref)) if ref == consumerActor =>
      failStage(new ConsumerFailed)
  }

  protected def onDownstreamFinishSubSourceCancellationStrategy(): SubSourceCancellationStrategy =
    if (buffer.hasNext) {
      SeekToOffsetAndReEmit(buffer.next().offset())
    } else {
      ReEmit
    }

  override def postStop(): Unit = {
    onShutdown()
    super.postStop()
  }

  setHandler(
    shape.out,
    new OutHandler {
      override def onPull(): Unit = pump()
      override def onDownstreamFinish(cause: Throwable): Unit = {
        subSourceCancelledCb.invoke(tp -> onDownstreamFinishSubSourceCancellationStrategy())
        super.onDownstreamFinish(cause)
      }
    }
  )

  def performShutdown(cause: Throwable) = {
    log.info("Completing ({}). Partition {}", cause, tp)
    completeStage()
  }

  @tailrec
  private def pump(): Unit =
    if (isAvailable(shape.out)) {
      if (buffer.hasNext) {
        val msg = buffer.next()
        push(shape.out, createMessage(msg))
        pump()
      } else if (!requested) {
        requested = true
        consumerActor.tell(requestMessages, subSourceActor.ref)
      }
    }
}
