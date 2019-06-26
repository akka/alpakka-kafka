/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import java.util.regex.Pattern

import akka.Done
import akka.actor.Status.Failure
import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  DeadLetterSuppression,
  NoSerializationVerificationNeeded,
  Status,
  Terminated,
  Timers
}
import akka.annotation.InternalApi
import akka.util.JavaDurationConverters._
import akka.event.LoggingReceive
import akka.kafka.KafkaConsumerActor.StoppingException
import akka.kafka._
import akka.stream.stage.AsyncCallback
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Internal API.
 *
 * The actor communicating through the Kafka consumer client.
 */
@InternalApi private object KafkaConsumerActor {

  object Internal {
    sealed trait SubscriptionRequest

    //requests
    final case class Assign(tps: Set[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class AssignWithOffset(tps: Map[TopicPartition, Long]) extends NoSerializationVerificationNeeded
    final case class AssignOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long])
        extends NoSerializationVerificationNeeded
    final case class Subscribe(topics: Set[String], listener: ListenerCallbacks)
        extends SubscriptionRequest
        with NoSerializationVerificationNeeded
    case object RequestMetrics extends NoSerializationVerificationNeeded
    // Could be optimized to contain a Pattern as it used during reconciliation now, tho only in exceptional circumstances
    final case class SubscribePattern(pattern: String, listener: ListenerCallbacks)
        extends SubscriptionRequest
        with NoSerializationVerificationNeeded
    final case class Seek(tps: Map[TopicPartition, Long]) extends NoSerializationVerificationNeeded
    final case class RequestMessages(requestId: Int, topics: Set[TopicPartition])
        extends NoSerializationVerificationNeeded
    val Stop = akka.kafka.KafkaConsumerActor.Stop
    final case class Commit(offsets: Map[TopicPartition, OffsetAndMetadata]) extends NoSerializationVerificationNeeded
    //responses
    final case class Assigned(partition: List[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class Revoked(partition: List[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class Messages[K, V](requestId: Int, messages: Iterator[ConsumerRecord[K, V]])
        extends NoSerializationVerificationNeeded
    final case class Committed(offsets: Map[TopicPartition, OffsetAndMetadata])
        extends NoSerializationVerificationNeeded
    final case class ConsumerMetrics(metrics: Map[MetricName, Metric]) extends NoSerializationVerificationNeeded {
      def getMetrics: java.util.Map[MetricName, Metric] = metrics.asJava
    }
    //internal
    private[KafkaConsumerActor] final case class Poll[K, V](
        target: KafkaConsumerActor[K, V],
        periodic: Boolean
    ) extends DeadLetterSuppression
        with NoSerializationVerificationNeeded

    private[KafkaConsumerActor] case object PollTask

    private val number = new AtomicInteger()
    def nextNumber(): Int =
      number.incrementAndGet()

  }

  final case class ListenerCallbacks(onAssign: Set[TopicPartition] => Unit, onRevoke: Set[TopicPartition] => Unit)
      extends NoSerializationVerificationNeeded

  object ListenerCallbacks {
    def apply(subscription: AutoSubscription,
              sourceActor: ActorRef,
              partitionAssignedCB: AsyncCallback[Set[TopicPartition]],
              partitionRevokedCB: AsyncCallback[Set[TopicPartition]],
              revokedBlockingCallback: Set[TopicPartition] => Unit = _ => ()): ListenerCallbacks =
      KafkaConsumerActor.ListenerCallbacks(
        assignedTps => {
          subscription.rebalanceListener.foreach {
            _.tell(TopicPartitionsAssigned(subscription, assignedTps), sourceActor)
          }
          if (assignedTps.nonEmpty) {
            partitionAssignedCB.invoke(assignedTps)
          }
        },
        revokedTps => {
          subscription.rebalanceListener.foreach {
            _.tell(TopicPartitionsRevoked(subscription, revokedTps), sourceActor)
          }
          if (revokedTps.nonEmpty) {
            partitionRevokedCB.invoke(revokedTps)
          }
          revokedBlockingCallback(revokedTps)
        }
      )
  }

  private[KafkaConsumerActor] trait CommitRefreshing {
    def add(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit
    def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit
    def revoke(revokedTps: Set[TopicPartition]): Unit
    def refreshOffsets: Map[TopicPartition, OffsetAndMetadata]
    def updateRefreshDeadlines(tps: Set[TopicPartition]): Unit
    def assignedPositions(assignedTps: Set[TopicPartition], assignedOffsets: Map[TopicPartition, Long]): Unit
    def assignedPositions(assignedTps: Set[TopicPartition],
                          consumer: Consumer[_, _],
                          positionTimeout: java.time.Duration): Unit
  }

  private[KafkaConsumerActor] object CommitRefreshing {
    def apply(commitRefreshInterval: Duration): CommitRefreshing =
      commitRefreshInterval match {
        case finite: FiniteDuration => new Impl(finite)
        case _ => NoOp
      }

    private object NoOp extends CommitRefreshing {
      def add(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {}
      def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {}
      def revoke(revokedTps: Set[TopicPartition]): Unit = {}
      val refreshOffsets: Map[TopicPartition, OffsetAndMetadata] = Map.empty
      def updateRefreshDeadlines(tps: Set[TopicPartition]): Unit = {}
      def assignedPositions(assignedTps: Set[TopicPartition], assignedOffsets: Map[TopicPartition, Long]): Unit = {}
      def assignedPositions(assignedTps: Set[TopicPartition],
                            consumer: Consumer[_, _],
                            positionTimeout: java.time.Duration): Unit = {}
    }

    private final class Impl(commitRefreshInterval: FiniteDuration) extends CommitRefreshing {
      private var requestedOffsets: Map[TopicPartition, OffsetAndMetadata] =
        Map.empty[TopicPartition, OffsetAndMetadata]
      private var committedOffsets: Map[TopicPartition, OffsetAndMetadata] =
        Map.empty[TopicPartition, OffsetAndMetadata]
      private var refreshDeadlines: Map[TopicPartition, Deadline] = Map.empty[TopicPartition, Deadline]

      def add(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit =
        requestedOffsets = requestedOffsets ++ offsets

      def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit =
        committedOffsets = committedOffsets ++ offsets

      def revoke(revokedTps: Set[TopicPartition]): Unit = {
        requestedOffsets = requestedOffsets -- revokedTps
        committedOffsets = committedOffsets -- revokedTps
        refreshDeadlines = refreshDeadlines -- revokedTps
      }

      def refreshOffsets: Map[TopicPartition, OffsetAndMetadata] = {
        val overdueTps = refreshDeadlines.filter(_._2.isOverdue()).keySet
        if (overdueTps.nonEmpty) {
          committedOffsets.filter {
            case (tp, offset) if overdueTps.contains(tp) =>
              requestedOffsets.get(tp).contains(offset)
            case _ =>
              false
          }
        } else {
          Map.empty
        }
      }

      def updateRefreshDeadlines(tps: Set[TopicPartition]): Unit =
        refreshDeadlines = refreshDeadlines ++ tps.map(_ -> commitRefreshInterval.fromNow)

      def assignedPositions(assignedTps: Set[TopicPartition], assignedOffsets: Map[TopicPartition, Long]): Unit = {
        requestedOffsets = requestedOffsets ++ assignedOffsets.map {
            case (partition, offset) =>
              partition -> requestedOffsets.getOrElse(partition, new OffsetAndMetadata(offset))
          }
        committedOffsets = committedOffsets ++ assignedOffsets.map {
            case (partition, offset) =>
              partition -> committedOffsets.getOrElse(partition, new OffsetAndMetadata(offset))
          }
        updateRefreshDeadlines(assignedTps)
      }

      def assignedPositions(assignedTps: Set[TopicPartition],
                            consumer: Consumer[_, _],
                            positionTimeout: java.time.Duration): Unit = {
        val assignedOffsets = assignedTps.map(tp => tp -> consumer.position(tp, positionTimeout)).toMap
        assignedPositions(assignedTps, assignedOffsets)
      }

    }
  }

  private val oneMilli = java.time.Duration.ofMillis(1)
}

/**
 * Internal API.
 *
 * The actor communicating through the Kafka consumer client.
 */
@InternalApi final private[kafka] class KafkaConsumerActor[K, V](owner: Option[ActorRef],
                                                                 settings: ConsumerSettings[K, V])
    extends Actor
    with ActorLogging
    with Timers {
  import KafkaConsumerActor.Internal._
  import KafkaConsumerActor._

  private val pollMsg = Poll(this, periodic = true)
  private val delayedPollMsg = Poll(this, periodic = false)
  private val pollTimeout = settings.pollTimeout.asJava

  /** Limits the blocking on offsetForTimes */
  private val offsetForTimesTimeout = settings.getOffsetForTimesTimeout

  /** Limits the blocking on position in [[WrappedAutoPausedListener]] */
  private val positionTimeout = settings.getPositionTimeout

  private var requests = Map.empty[ActorRef, RequestMessages]
  private var requestors = Set.empty[ActorRef]
  private var consumer: Consumer[K, V] = _
  private var subscriptions = Set.empty[SubscriptionRequest]
  private var commitsInProgress = 0
  private val commitRefreshing = CommitRefreshing(settings.commitRefreshInterval)
  private var stopInProgress = false

  settings.connectionCheckerSettings match {
    case settings: EnabledConnectionCheckerSettings => context.actorOf(ConnectionChecker.props(settings))
    case DisabledConnectionCheckerSettings => ()
  }

  /**
   * While `true`, committing is delayed.
   * Changed by `onPartitionsRevoked` and `onPartitionsAssigned` in [[WrappedAutoPausedListener]].
   */
  private var rebalanceInProgress = false

  /**
   * Keeps commit offsets during rebalances for later commit.
   */
  private var rebalanceCommitStash = Map.empty[TopicPartition, OffsetAndMetadata]

  /**
   * Keeps commit senders that need a reply once stashed commits are made.
   */
  private var rebalanceCommitSenders = Vector.empty[ActorRef]

  private var delayedPollInFlight = false

  def receive: Receive = LoggingReceive {
    case Assign(assignedTps) =>
      scheduleFirstPollTask()
      checkOverlappingRequests("Assign", sender(), assignedTps)
      val previousAssigned = consumer.assignment()
      consumer.assign((assignedTps.toSeq ++ previousAssigned.asScala).asJava)
      commitRefreshing.assignedPositions(assignedTps, consumer, positionTimeout)

    case AssignWithOffset(assignedOffsets) =>
      scheduleFirstPollTask()
      checkOverlappingRequests("AssignWithOffset", sender(), assignedOffsets.keySet)
      val previousAssigned = consumer.assignment()
      consumer.assign((assignedOffsets.keys.toSeq ++ previousAssigned.asScala).asJava)
      assignedOffsets.foreach {
        case (tp, offset) =>
          consumer.seek(tp, offset)
      }
      commitRefreshing.assignedPositions(assignedOffsets.keySet, assignedOffsets)

    case AssignOffsetsForTimes(timestampsToSearch) =>
      scheduleFirstPollTask()
      checkOverlappingRequests("AssignOffsetsForTimes", sender(), timestampsToSearch.keySet)
      val previousAssigned = consumer.assignment()
      consumer.assign((timestampsToSearch.keys.toSeq ++ previousAssigned.asScala).asJava)
      val topicPartitionToOffsetAndTimestamp =
        consumer.offsetsForTimes(timestampsToSearch.mapValues(long2Long).toMap.asJava, offsetForTimesTimeout)
      val assignedOffsets = topicPartitionToOffsetAndTimestamp.asScala.filter(_._2 != null).toMap.map {
        case (tp, oat: OffsetAndTimestamp) =>
          val offset = oat.offset()
          val ts = oat.timestamp()
          log.debug("Get offset {} from topic {} with timestamp {}", offset, tp, ts)
          consumer.seek(tp, offset)
          tp -> offset
      }
      commitRefreshing.assignedPositions(assignedOffsets.keySet, assignedOffsets)

    case Commit(offsets) if rebalanceInProgress =>
      rebalanceCommitStash ++= offsets
      rebalanceCommitSenders = rebalanceCommitSenders :+ sender()

    case Commit(offsets) =>
      commitRefreshing.add(offsets)
      val replyTo = sender()
      commit(offsets, replyTo ! _)

    case s: SubscriptionRequest =>
      subscriptions = subscriptions + s
      handleSubscription(s)

    case Seek(offsets) =>
      offsets.foreach { case (tp, offset) => consumer.seek(tp, offset) }
      sender() ! Done

    case p: Poll[_, _] =>
      receivePoll(p)

    case req: RequestMessages =>
      context.watch(sender())
      checkOverlappingRequests("RequestMessages", sender(), req.topics)
      requests = requests.updated(sender(), req)
      requestors += sender()
      // When many requestors, e.g. many partitions with committablePartitionedSource the
      // performance is much by collecting more requests/commits before performing the poll.
      // That is done by sending a message to self, and thereby collect pending messages in mailbox.
      if (requestors.size == 1)
        poll()
      else if (!delayedPollInFlight) {
        delayedPollInFlight = true
        self ! delayedPollMsg
      }

    case Committed(offsets) =>
      commitRefreshing.committed(offsets)

    case Stop =>
      if (commitsInProgress == 0) {
        log.debug("Received Stop from {}, stopping", sender())
        context.stop(self)
      } else {
        log.debug("Received Stop from {}, waiting for commitsInProgress={}", sender(), commitsInProgress)
        stopInProgress = true
        context.become(stopping)
      }

    case kcf: KafkaConnectionFailed =>
      processErrors(kcf)
      self ! Stop

    case RequestMetrics =>
      val unmodifiableYetMutableMetrics: java.util.Map[MetricName, _ <: Metric] = consumer.metrics()
      sender() ! ConsumerMetrics(unmodifiableYetMutableMetrics.asScala.toMap)

    case Terminated(ref) =>
      requests -= ref
      requestors -= ref

    case req: Metadata.Request =>
      sender ! handleMetadataRequest(req)
  }

  def handleSubscription(subscription: SubscriptionRequest): Unit =
    try {
      subscription match {
        case Subscribe(topics, listener) =>
          consumer.subscribe(topics.toList.asJava, new WrappedAutoPausedListener(listener))
        case SubscribePattern(pattern, listener) =>
          consumer.subscribe(Pattern.compile(pattern), new WrappedAutoPausedListener(listener))
      }

      scheduleFirstPollTask()
    } catch {
      case NonFatal(ex) => processErrors(ex)
    }

  def checkOverlappingRequests(updateType: String, fromStage: ActorRef, topics: Set[TopicPartition]): Unit =
    // check if same topics/partitions have already been requested by someone else,
    // which is an indication that something is wrong, but it might be alright when assignments change.
    if (requests.nonEmpty) requests.foreach {
      case (ref, r) =>
        if (ref != fromStage && r.topics.exists(topics.apply)) {
          log.warning("{} from topic/partition {} already requested by other stage {}", updateType, topics, r.topics)
          ref ! Messages(r.requestId, Iterator.empty)
          requests -= ref
        }
    }

  def stopping: Receive = LoggingReceive {
    case p: Poll[_, _] =>
      receivePoll(p)
    case Stop =>
    case _: Terminated =>
    case _ @(_: Commit | _: RequestMessages) =>
      sender() ! Status.Failure(StoppingException())
    case msg @ (_: Assign | _: AssignWithOffset | _: Subscribe | _: SubscribePattern) =>
      log.warning("Got unexpected message {} when KafkaConsumerActor is in stopping state", msg)
  }

  override def preStart(): Unit = {
    super.preStart()
    try {
      if (log.isDebugEnabled)
        log.debug(s"Creating Kafka consumer with ${settings.toString}")
      consumer = settings.createKafkaConsumer()
    } catch {
      case e: Exception =>
        owner.foreach(_ ! Failure(e))
        throw e
    }
  }

  override def postStop(): Unit = {
    // reply to outstanding requests is important if the actor is restarted
    requests.foreach {
      case (ref, req) =>
        ref ! Messages(req.requestId, Iterator.empty)
    }
    consumer.close(settings.getCloseTimeout)
    super.postStop()
  }

  def scheduleFirstPollTask(): Unit =
    if (!timers.isTimerActive(PollTask)) schedulePollTask()

  def schedulePollTask(): Unit =
    timers.startSingleTimer(PollTask, pollMsg, settings.pollInterval)

  private def receivePoll(p: Poll[_, _]): Unit =
    if (p.target == this) {
      val refreshOffsets = commitRefreshing.refreshOffsets
      if (refreshOffsets.nonEmpty) {
        log.debug("Refreshing committed offsets: {}", refreshOffsets)
        commit(refreshOffsets, context.system.deadLetters ! _)
      }
      poll()
      if (p.periodic)
        schedulePollTask()
      else
        delayedPollInFlight = false
    } else {
      // Message was enqueued before a restart - can be ignored
      log.debug("Ignoring Poll message with stale target ref")
    }

  def poll(): Unit = {
    val currentAssignmentsJava = consumer.assignment()
    val initialRebalanceInProgress = rebalanceInProgress
    try {
      if (requests.isEmpty) {
        // no outstanding requests so we don't expect any messages back, but we should anyway
        // drive the KafkaConsumer by polling
        def checkNoResult(rawResult: ConsumerRecords[K, V]): Unit =
          if (!rawResult.isEmpty)
            throw new IllegalStateException(s"Got ${rawResult.count} unexpected messages")
        consumer.pause(currentAssignmentsJava)
        checkNoResult(consumer.poll(java.time.Duration.ZERO))

        // COMMIT PERFORMANCE OPTIMIZATION
        // For commits we try to avoid blocking poll because a commit normally succeeds after a few
        // poll(0). Using poll(1) will always block for 1 ms, since there are no messages.
        // Therefore we do 10 poll(0) with short 10 μs delay followed by 1 poll(1).
        // If it's still not completed it will be tried again after the scheduled Poll.
        var i = 10
        while (i > 0 && commitsInProgress > 0) {
          LockSupport.parkNanos(10 * 1000)
          val pollTimeout = if (i == 1) oneMilli else java.time.Duration.ZERO
          checkNoResult(consumer.poll(pollTimeout))
          i -= 1
        }
      } else {
        // resume partitions to fetch
        val partitionsToFetch: Set[TopicPartition] = requests.values.flatMap(_.topics).toSet
        val (resumeThese, pauseThese) = currentAssignmentsJava.asScala.partition(partitionsToFetch.contains)
        consumer.pause(pauseThese.asJava)
        consumer.resume(resumeThese.asJava)
        processResult(partitionsToFetch, consumer.poll(pollTimeout))
      }
    } catch {
      case e: org.apache.kafka.common.errors.SerializationException =>
        processErrors(e)
      case NonFatal(e) =>
        processErrors(e)
        log.error(e, "Exception when polling from consumer, stopping actor: {}", e.toString)
        context.stop(self)
    }
    checkRebalanceState(initialRebalanceInProgress)

    if (stopInProgress && commitsInProgress == 0) {
      log.debug("Stopping")
      context.stop(self)
    }
  }

  private def commit(commitMap: Map[TopicPartition, OffsetAndMetadata], sendReply: AnyRef => Unit): Unit = {
    commitRefreshing.updateRefreshDeadlines(commitMap.keySet)
    commitsInProgress += 1
    val startTime = System.nanoTime()
    consumer.commitAsync(
      commitMap.asJava,
      new OffsetCommitCallback {
        override def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata],
                                exception: Exception): Unit = {
          // this is invoked on the thread calling consumer.poll which will always be the actor, so it is safe
          val duration = System.nanoTime() - startTime
          if (duration > settings.commitTimeWarning.toNanos) {
            log.warning("Kafka commit took longer than `commit-time-warning`: {} ms", duration / 1000000L)
          }
          commitsInProgress -= 1
          if (exception != null) sendReply(Status.Failure(exception))
          else {
            self ! Committed(offsets.asScala.toMap)
            sendReply(Done)
          }
        }
      }
    )
    // When many requestors, e.g. many partitions with committablePartitionedSource the
    // performance is much by collecting more requests/commits before performing the poll.
    // That is done by sending a message to self, and thereby collect pending messages in mailbox.
    if (requestors.size == 1)
      poll()
    else if (!delayedPollInFlight) {
      delayedPollInFlight = true
      self ! delayedPollMsg
    }
  }

  private def processResult(partitionsToFetch: Set[TopicPartition], rawResult: ConsumerRecords[K, V]): Unit =
    if (!rawResult.isEmpty) {
      //check the we got only requested partitions and did not drop any messages
      val fetchedTps = rawResult.partitions().asScala
      if ((fetchedTps diff partitionsToFetch).nonEmpty)
        throw new scala.IllegalArgumentException(
          s"Unexpected records polled. Expected: $partitionsToFetch, " +
          s"result: ${rawResult.partitions()}, consumer assignment: ${consumer.assignment()}"
        )

      //send messages to actors
      requests.foreach {
        case (stageActorRef, req) =>
          //gather all messages for ref
          val messages = req.topics.foldLeft[Iterator[ConsumerRecord[K, V]]](Iterator.empty) {
            case (acc, tp) =>
              val tpMessages = rawResult.records(tp).asScala.iterator
              if (acc.isEmpty) tpMessages
              else acc ++ tpMessages
          }
          if (messages.nonEmpty) {
            stageActorRef ! Messages(req.requestId, messages)
            requests -= stageActorRef
          }
      }
    }

  private def processErrors(exception: Throwable): Unit = {
    val involvedStageActors = (requests.keys ++ owner).toSet
    log.debug("sending failure to {}", involvedStageActors.mkString(","))
    involvedStageActors.foreach { stageActorRef =>
      stageActorRef ! Failure(exception)
      requests -= stageActorRef
    }
  }

  private def handleMetadataRequest(req: Metadata.Request): Metadata.Response = req match {
    case Metadata.ListTopics =>
      Metadata.Topics(Try {
        consumer
          .listTopics(settings.getMetadataRequestTimeout)
          .asScala
          .map {
            case (k, v) => k -> v.asScala.toList
          }
          .toMap
      })

    case Metadata.GetPartitionsFor(topic) =>
      Metadata.PartitionsFor(Try {
        consumer.partitionsFor(topic, settings.getMetadataRequestTimeout).asScala.toList
      })

    case Metadata.GetBeginningOffsets(partitions) =>
      Metadata.BeginningOffsets(Try {
        consumer
          .beginningOffsets(partitions.asJava, settings.getMetadataRequestTimeout)
          .asScala
          .map {
            case (k, v) => k -> (v: Long)
          }
          .toMap
      })

    case Metadata.GetEndOffsets(partitions) =>
      Metadata.EndOffsets(Try {
        consumer
          .endOffsets(partitions.asJava, settings.getMetadataRequestTimeout)
          .asScala
          .map {
            case (k, v) => k -> (v: Long)
          }
          .toMap
      })

    case Metadata.GetOffsetsForTimes(timestampsToSearch) =>
      Metadata.OffsetsForTimes(Try {
        val search = timestampsToSearch.map {
          case (k, v) => k -> (v: java.lang.Long)
        }.asJava
        consumer.offsetsForTimes(search, settings.getMetadataRequestTimeout).asScala.toMap
      })

    case Metadata.GetCommittedOffset(partition) =>
      Metadata.CommittedOffset(
        Try { consumer.committed(partition, settings.getMetadataRequestTimeout) },
        partition
      )
  }

  /**
   * Detects state changes of [[rebalanceInProgress]] and takes action on it.
   */
  private def checkRebalanceState(initialRebalanceInProgress: Boolean): Unit =
    if (initialRebalanceInProgress && !rebalanceInProgress && rebalanceCommitSenders.nonEmpty) {
      log.debug("committing stash {} replying to {}", rebalanceCommitStash, rebalanceCommitSenders)
      val replyTo = rebalanceCommitSenders
      commit(rebalanceCommitStash, msg => replyTo.foreach(_ ! msg))
      rebalanceCommitStash = Map.empty
      rebalanceCommitSenders = Vector.empty
    }

  /**
   * Copied from the implemented interface: "
   * These methods will be called after the partition re-assignment completes and before the
   * consumer starts fetching data, and only as the result of a `poll` call.
   *
   * It is guaranteed that all the processes in a consumer group will execute their
   * `onPartitionsRevoked` callback before any instance executes its
   * `onPartitionsAssigned` callback.
   * "
   *
   * So these methods are always called on the same thread as the actor and we're safe to
   * touch internal state.
   */
  private final class WrappedAutoPausedListener(listener: ListenerCallbacks)
      extends ConsumerRebalanceListener
      with NoSerializationVerificationNeeded {

    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      consumer.pause(partitions)
      val tps = partitions.asScala.toSet
      commitRefreshing.assignedPositions(tps, consumer, positionTimeout)
      listener.onAssign(tps)
      rebalanceInProgress = false
    }

    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      val revokedTps = partitions.asScala.toSet
      listener.onRevoke(revokedTps)
      commitRefreshing.revoke(revokedTps)
      rebalanceInProgress = true
    }
  }

}
