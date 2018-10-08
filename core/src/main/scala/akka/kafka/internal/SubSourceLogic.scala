/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.annotation.InternalApi
import akka.kafka.Subscriptions.{TopicSubscription, TopicSubscriptionPattern}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka._
import akka.pattern.{ask, AskTimeoutException}
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{StageLogging, _}
import akka.stream.{ActorMaterializerHelper, Attributes, Outlet, SourceShape}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Internal API
 */
@InternalApi
private[kafka] abstract class SubSourceLogic[K, V, Msg](
    val shape: SourceShape[(TopicPartition, Source[Msg, NotUsed])],
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription,
    getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]] = None,
    onRevoke: Set[TopicPartition] => Unit = _ => ()
) extends TimerGraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with MessageBuilder[K, V, Msg]
    with StageLogging {
  import SubSourceLogic._

  val consumerPromise = Promise[ActorRef]
  final val actorNumber = KafkaConsumerActor.Internal.nextNumber()
  override def executionContext: ExecutionContext = materializer.executionContext
  override def consumerFuture: Future[ActorRef] = consumerPromise.future
  var consumerActor: ActorRef = _
  var sourceActor: StageActor = _

  /** Kafka has notified us that we have these partitions assigned, but we have not created a source for them yet. */
  var pendingPartitions: immutable.Set[TopicPartition] = immutable.Set.empty

  /** We have created a source for these partitions, but it has not started up and is not in subSources yet. */
  var partitionsInStartup: immutable.Set[TopicPartition] = immutable.Set.empty
  var subSources: Map[TopicPartition, Control] = immutable.Map.empty

  /** Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking. */
  var partitionsToRevoke: Set[TopicPartition] = Set.empty

  override def preStart(): Unit = {
    super.preStart()
    consumerActor = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      extendedActorSystem.systemActorOf(akka.kafka.KafkaConsumerActor.props(settings), s"kafka-consumer-$actorNumber")
    }
    consumerPromise.success(consumerActor)

    sourceActor = getStageActor {
      case (_, Terminated(ref)) if ref == consumerActor =>
        failStage(new ConsumerFailed)
    }
    sourceActor.watch(consumerActor)

    def rebalanceListener =
      KafkaConsumerActor.ListenerCallbacks(partitionAssignedCB.invoke, partitionRevokedCB.invoke)

    subscription match {
      case TopicSubscription(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener), sourceActor.ref)
      case TopicSubscriptionPattern(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener), sourceActor.ref)
    }
  }

  private val updatePendingPartitionsAndEmitSubSourcesCb =
    getAsyncCallback[Set[TopicPartition]](updatePendingPartitionsAndEmitSubSources)

  private val stageFailCB = getAsyncCallback[ConsumerFailed] { ex =>
    failStage(ex)
  }

  val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { assigned =>
    subscription.rebalanceListener.foreach {
      _.tell(TopicPartitionsAssigned(subscription, assigned), sourceActor.ref)
    }
    val formerlyUnknown = assigned -- partitionsToRevoke

    if (log.isDebugEnabled && formerlyUnknown.nonEmpty) {
      log.debug("#{} Assigning new partitions: {}", actorNumber, formerlyUnknown.mkString(", "))
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
                  s"#$actorNumber Failed to fetch offset for partitions: ${formerlyUnknown.mkString(", ")}.",
                  ex
                )
              )
            case Success(offsets) =>
              seekAndEmitSubSources(formerlyUnknown, offsets)
          }
    }
  }

  private def seekAndEmitSubSources(
      formerlyUnknown: Set[TopicPartition],
      offsets: Map[TopicPartition, Long]
  ): Unit = {
    implicit val ec: ExecutionContext = materializer.executionContext
    val seekTimeout: Timeout = Timeout(10000, TimeUnit.MILLISECONDS)
    consumerActor
      .ask(KafkaConsumerActor.Internal.Seek(offsets))(seekTimeout, sourceActor.ref)
      .map(_ => updatePendingPartitionsAndEmitSubSourcesCb.invoke(formerlyUnknown))
      .recover {
        case _: AskTimeoutException =>
          stageFailCB.invoke(
            new ConsumerFailed(
              s"#$actorNumber Consumer failed during seek for partitions: ${offsets.keys.mkString(", ")}."
            )
          )
      }
  }

  val partitionRevokedCB = getAsyncCallback[Set[TopicPartition]] { revoked =>
    subscription.rebalanceListener.foreach {
      _.tell(TopicPartitionsRevoked(subscription, revoked), sourceActor.ref)
    }
    // TODO this called in startup with empty tps, some existing tests rely on the callback
    partitionsToRevoke ++= revoked
    scheduleOnce(CloseRevokedPartitions, settings.waitClosePartition)
  }

  override def onTimer(timerKey: Any): Unit = timerKey match {
    case CloseRevokedPartitions =>
      if (log.isDebugEnabled) {
        log.debug("#{} Closing SubSources for revoked partitions: {}", actorNumber, partitionsToRevoke.mkString(", "))
      }
      onRevoke(partitionsToRevoke)
      pendingPartitions --= partitionsToRevoke
      partitionsInStartup --= partitionsToRevoke
      partitionsToRevoke.flatMap(subSources.get).foreach(_.shutdown())
      subSources --= partitionsToRevoke
      partitionsToRevoke = Set.empty
  }

  val subsourceCancelledCB: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])] =
    getAsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])] {
      case (tp, firstUnconsumed) =>
        subSources -= tp
        partitionsInStartup -= tp
        pendingPartitions += tp
        firstUnconsumed match {
          case Some(record) =>
            if (log.isDebugEnabled) {
              log.debug("#{} Seeking {} to {} after partition SubSource cancelled", actorNumber, tp, record.offset())
            }
            seekAndEmitSubSources(formerlyUnknown = Set.empty, Map(tp -> record.offset()))
          case None => emitSubSourcesForPendingPartitions()
        }
    }

  val subsourceStartedCB: AsyncCallback[(TopicPartition, Control)] = getAsyncCallback[(TopicPartition, Control)] {
    case (tp, control) =>
      if (!partitionsInStartup.contains(tp)) {
        // Partition was revoked while
        // starting up.  Kill!
        control.shutdown()
      } else {
        subSources += (tp -> control)
        partitionsInStartup -= tp
      }
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit =
      emitSubSourcesForPendingPartitions()
    override def onDownstreamFinish(): Unit =
      performShutdown()
  })

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
                           messageBuilder = this,
                           actorNumber)
      )
      push(shape.out, (tp, subSource))
      emitSubSourcesForPendingPartitions()
    }

  override def postStop(): Unit = {
    consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    onShutdown()
    super.postStop()
  }

  override def performStop(): Unit = {
    setKeepGoing(true)
    subSources.foreach {
      case (_, control) => control.stop()
    }
    complete(shape.out)
    onStop()
  }

  override def performShutdown(): Unit = {
    setKeepGoing(true)
    //todo we should wait for subsources to be shutdown and next shutdown main stage
    subSources.foreach {
      case (_, control) => control.shutdown()
    }

    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    sourceActor.become {
      case (_, Terminated(ref)) if ref == consumerActor =>
        onShutdown()
        completeStage()
    }
    consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
  }

}

private object SubSourceLogic {
  case object CloseRevokedPartitions
}

private final class SubSourceStage[K, V, Msg](
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[(TopicPartition, Control)],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])],
    messageBuilder: MessageBuilder[K, V, Msg],
    actorNumber: Int
) extends GraphStage[SourceShape[Msg]] { stage =>
  val out = Outlet[Msg]("out")
  val shape = new SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with PromiseControl with MetricsControl with StageLogging {
      override def executionContext: ExecutionContext = materializer.executionContext
      override def consumerFuture: Future[ActorRef] = Future.successful(consumerActor)
      val shape = stage.shape
      val requestMessages = KafkaConsumerActor.Internal.RequestMessages(0, Set(tp))
      var requested = false
      var subSourceActor: StageActor = _
      var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

      override def preStart(): Unit = {
        log.debug("#{} Starting SubSource for partition {}", actorNumber, tp)
        super.preStart()
        subSourceStartedCb.invoke(tp -> this.asInstanceOf[Control])
        subSourceActor = getStageActor {
          case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
            requested = false
            // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
            if (buffer.hasNext) {
              buffer = buffer ++ msg.messages
            } else {
              buffer = msg.messages
            }
            pump()
          case (_, Terminated(ref)) if ref == consumerActor =>
            failStage(new ConsumerFailed)
        }
        subSourceActor.watch(consumerActor)
      }

      override def postStop(): Unit = {
        onShutdown()
        super.postStop()
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            pump()

          override def onDownstreamFinish(): Unit = {
            val firstUnconsumed = if (buffer.hasNext) {
              Some(buffer.next())
            } else {
              None
            }

            subSourceCancelledCb.invoke(tp -> firstUnconsumed)
            super.onDownstreamFinish()
          }
        }
      )

      def performShutdown() = {
        log.debug("#{} Completing SubSource for partition {}", actorNumber, tp)
        completeStage()
      }

      @tailrec
      private def pump(): Unit =
        if (isAvailable(out)) {
          if (buffer.hasNext) {
            val msg = buffer.next()
            push(out, messageBuilder.createMessage(msg))
            pump()
          } else if (!requested) {
            requested = true
            consumerActor.tell(requestMessages, subSourceActor.ref)
          }
        }
    }
}
