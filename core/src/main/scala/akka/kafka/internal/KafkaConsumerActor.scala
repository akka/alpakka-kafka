/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import java.util.regex.Pattern
import akka.Done
import akka.actor.Status.Failure
import akka.actor.{
  Actor,
  ActorRef,
  DeadLetterSuppression,
  NoSerializationVerificationNeeded,
  Stash,
  Status,
  Terminated,
  Timers
}
import akka.annotation.InternalApi
import akka.event.LoggingReceive
import akka.kafka.KafkaConsumerActor.{StopLike, StoppingException}
import akka.kafka._
import akka.kafka.scaladsl.PartitionAssignmentHandler
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.errors.{
  CoordinatorLoadInProgressException,
  RebalanceInProgressException,
  TimeoutException
}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.util.{Success, Try}
import scala.util.control.NonFatal

/**
 * Internal API.
 *
 * The actor communicating through the Kafka consumer client.
 */
@InternalApi private object KafkaConsumerActor {

  object Internal {
    sealed trait SubscriptionRequest extends NoSerializationVerificationNeeded

    //requests
    final case class Assign(tps: Set[TopicPartition]) extends SubscriptionRequest
    final case class AssignWithOffset(tps: Map[TopicPartition, Long]) extends SubscriptionRequest
    final case class AssignOffsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) extends SubscriptionRequest
    final case class Subscribe(topics: Set[String], rebalanceHandler: PartitionAssignmentHandler)
        extends SubscriptionRequest
    case object RequestMetrics extends NoSerializationVerificationNeeded
    // Could be optimized to contain a Pattern as it used during reconciliation now, tho only in exceptional circumstances
    final case class SubscribePattern(pattern: String, rebalanceHandler: PartitionAssignmentHandler)
        extends SubscriptionRequest
    final case class RegisterSubStage(tps: Set[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class Seek(tps: Map[TopicPartition, Long]) extends NoSerializationVerificationNeeded
    final case class RequestMessages(requestId: Int, tps: Set[TopicPartition]) extends NoSerializationVerificationNeeded
    val Stop = akka.kafka.KafkaConsumerActor.Stop
    final case class StopFromStage(stageId: String) extends StopLike
    final case class Commit(tp: TopicPartition, offsetAndMetadata: OffsetAndMetadata)
        extends NoSerializationVerificationNeeded
    final case class CommitWithoutReply(tp: TopicPartition, offsetAndMetadata: OffsetAndMetadata, emergency: Boolean)
        extends NoSerializationVerificationNeeded

    /** Special case commit for non-batched committing. */
    final case class CommitSingle(tp: TopicPartition, offsetAndMetadata: OffsetAndMetadata)
        extends NoSerializationVerificationNeeded

    // Used for transactions/EOS periodically and when needed, pushes the current ConsumerGroupMetadata to the subscriber (producer stage)
    case class SubscribeToGroupMetaData(subscriber: ActorRef) extends NoSerializationVerificationNeeded
    case object GroupMetadataTick extends NoSerializationVerificationNeeded

    //responses
    final case class Assigned(partition: List[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class Revoked(partition: List[TopicPartition]) extends NoSerializationVerificationNeeded
    final case class Messages[K, V](requestId: Int, messages: Iterator[ConsumerRecord[K, V]])
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

  private[KafkaConsumerActor] trait CommitRefreshing {
    def updateRefreshDeadlines(tps: Set[TopicPartition]): Unit
    def refreshOffsets: Map[TopicPartition, OffsetAndMetadata]
  }

  private[KafkaConsumerActor] object CommitRefreshing {
    def apply(commitRefreshInterval: Duration, progress: () => ConsumerProgressTracking): CommitRefreshing =
      commitRefreshInterval match {
        case finite: FiniteDuration => new Impl(finite, progress())
        case _ => new Noop()
      }

    private final class Noop() extends CommitRefreshing {
      override def updateRefreshDeadlines(tps: Set[TopicPartition]): Unit = {}

      override val refreshOffsets: Map[TopicPartition, OffsetAndMetadata] = Map()
    }
    private final class Impl(commitRefreshInterval: FiniteDuration, progress: ConsumerProgressTracking)
        extends CommitRefreshing
        with ConsumerAssignmentTrackingListener {
      progress.addProgressTrackingCallback(this)

      private var refreshDeadlines: Map[TopicPartition, Deadline] = Map.empty[TopicPartition, Deadline]

      override def revoke(revokedTps: Set[TopicPartition]): Unit = {
        refreshDeadlines = refreshDeadlines -- revokedTps
      }

      def refreshOffsets: Map[TopicPartition, OffsetAndMetadata] = {
        val overdueTps = refreshDeadlines.filter(_._2.isOverdue()).keySet
        if (overdueTps.nonEmpty) {
          progress.committedOffsets.filter {
            case (tp, offset) if overdueTps.contains(tp) =>
              progress.commitRequested.get(tp).contains(offset)
            case _ =>
              false
          }
        } else {
          Map.empty
        }
      }

      def updateRefreshDeadlines(tps: Set[TopicPartition]): Unit =
        // only update some of the deadlines, those that we have been assigned. The committed set that is expected to
        // be <= the number of assigned partitions, so intersect over that set. It is possible that we try to commit a
        // partition that is no longer assigned to this consumer, so that assumption is not necessarily strictly
        // true, but it's reasonable.
        refreshDeadlines = refreshDeadlines ++ tps.intersect(refreshDeadlines.keySet).map { tp =>
            (tp, commitRefreshInterval.fromNow)
          }

      override def assignedPositions(assignedTps: Set[TopicPartition],
                                     assignedOffsets: Map[TopicPartition, Long]): Unit = {
        // assigned the partitions, so update all the of deadlines
        refreshDeadlines = refreshDeadlines ++ assignedTps.map(_ -> commitRefreshInterval.fromNow)
      }
    }
  }

  private val oneMilli = java.time.Duration.ofMillis(1)

  /**
   * Create map with just the highest received offsets.
   */
  private[internal] def aggregateOffsets(cm: List[(TopicPartition, OffsetAndMetadata)]) =
    cm.foldLeft(Map.empty[TopicPartition, OffsetAndMetadata]) { (aggregate, add) =>
      val (tp, toBeAdded) = add
      if (aggregate.get(tp).exists(_.offset > toBeAdded.offset))
        aggregate
      else aggregate + add
    }
}

/**
 * Internal API.
 *
 * The actor communicating through the Kafka consumer client.
 */
@InternalApi final private[kafka] class KafkaConsumerActor[K, V](owner: Option[ActorRef],
                                                                 _settings: ConsumerSettings[K, V])
    extends Actor
    with ActorIdLogging
    with Timers
    with Stash {
  import KafkaConsumerActor.Internal._
  import KafkaConsumerActor._

  private val pollMsg = Poll(this, periodic = true)
  private val delayedPollMsg = Poll(this, periodic = false)

  private var settings: ConsumerSettings[K, V] = _
  private var pollTimeout: java.time.Duration = _

  /** Limits the blocking on offsetForTimes */
  private var offsetForTimesTimeout: java.time.Duration = _

  /** Limits the blocking on position in [[RebalanceListenerImpl]] */
  private var positionTimeout: java.time.Duration = _

  private var requests = Map.empty[ActorRef, RequestMessages]

  /** ActorRefs of all stages that sent subscriptions requests or `RegisterSubStage` to this actor (removed on their termination). */
  private var stageActorsMap = Map.empty[Set[TopicPartition], ActorRef]
  private var consumer: Consumer[K, V] = _
  private var commitsInProgress = 0
  private var commitRefreshing: CommitRefreshing = _
  private var resetProtection: ConsumerResetProtection = _
  private var stopInProgress = false

  private var metadataSubscribers = Set.empty[ActorRef]

  /**
   * Collect commit offset maps until the next poll.
   */
  private var commitMaps = List.empty[(TopicPartition, OffsetAndMetadata)]

  /**
   * Keeps commit senders that need a reply once stashed commits are made.
   */
  private var commitSenders = Vector.empty[ActorRef]

  private var delayedPollInFlight = false
  private var partitionAssignmentHandler: RebalanceListener = RebalanceListener.Empty
  private var progressTracker: ConsumerProgressTracking = ConsumerProgressTrackerNoop

  override val receive: Receive = regularReceive

  def regularReceive: Receive = LoggingReceive {
    case Commit(tp, offset) =>
      // prepending, as later received offsets most likely are higher
      commitMaps = tp -> offset :: commitMaps
      commitSenders = commitSenders :+ sender()

    case CommitWithoutReply(tp, offset, emergency) =>
      // prepending, as later received offsets most likely are higher
      commitMaps = tp -> offset :: commitMaps
      if (emergency) {
        emergencyPoll()
      }

    case CommitSingle(tp, offset) =>
      commitMaps = tp -> offset :: commitMaps
      commitSenders = commitSenders :+ sender()
      requestDelayedPoll()

    case s: SubscriptionRequest =>
      handleSubscription(s)

    case RegisterSubStage(tps) =>
      stageActorsMap = stageActorsMap.updated(tps, sender())

    case Seek(offsets) =>
      try {
        offsets.foreach { case (tp, offset) => consumer.seek(tp, offset) }
        sender() ! Done
      } catch {
        case NonFatal(e) => sendFailure(e, sender())
      }

    case p: Poll[_, _] =>
      receivePoll(p)

    case req: RequestMessages =>
      context.watch(sender())
      checkOverlappingRequests("RequestMessages", sender(), req.tps)
      // https://github.com/akka/alpakka-kafka/pull/1263
      if (stageActorsMap.getOrElse(req.tps, sender()) == sender())
        requests = requests.updated(sender(), req)
      if (stageActorsMap.size == 1)
        poll()
      else requestDelayedPoll()

    case s: StopLike =>
      val from = stopFromMessage(s)
      commitAggregatedOffsets()
      if (commitsInProgress == 0) {
        log.debug("Received Stop from {}, stopping", from)
        context.stop(self)
      } else {
        log.debug("Received Stop from {}, waiting for commitsInProgress={}", from, commitsInProgress)
        stopInProgress = true
        context.become(stopping)
      }

    case kcf: KafkaConnectionFailed =>
      processErrors(kcf)
      self ! Stop

    case RequestMetrics =>
      try {
        val unmodifiableYetMutableMetrics: java.util.Map[MetricName, _ <: Metric] = consumer.metrics()
        sender() ! ConsumerMetrics(unmodifiableYetMutableMetrics.asScala.toMap)
      } catch {
        case NonFatal(e) => sendFailure(e, sender())
      }

    case Terminated(ref) =>
      stageActorsMap = stageActorsMap.filterNot(_._2 == ref)
      requests -= ref

    case SubscribeToGroupMetaData(subscriber) =>
      if (metadataSubscribers.isEmpty)
        timers.startTimerAtFixedRate(GroupMetadataTick, GroupMetadataTick, _settings.consumerGroupUpdateInterval)
      metadataSubscribers += subscriber
      subscriber ! consumer.groupMetadata()

    case GroupMetadataTick =>
      sendConsumerGroupMetadataToSubscribers()

    case req: Metadata.Request =>
      sender() ! handleMetadataRequest(req)
  }

  private def sendConsumerGroupMetadataToSubscribers(): Unit = {
    val metaData = consumer.groupMetadata()
    metadataSubscribers.foreach(_ ! metaData)
  }

  def expectSettings: Receive = LoggingReceive.withLabel("expectSettings") {
    case s: ConsumerSettings[K @unchecked, V @unchecked] =>
      applySettings(s)

    case scala.util.Failure(e) =>
      owner.foreach(_ ! Failure(e))
      throw e

    case s: StopLike =>
      val from = stopFromMessage(s)
      log.debug("Received Stop from {}, stopping", from)
      context.stop(self)

    case _ =>
      stash()
  }

  def handleSubscription(subscription: SubscriptionRequest): Unit =
    try {
      subscription match {
        case Assign(assignedTps) =>
          checkOverlappingRequests("Assign", sender(), assignedTps)
          val previousAssigned = consumer.assignment()
          consumer.assign((assignedTps.toSeq ++ previousAssigned.asScala).asJava)
          progressTracker.assignedPositionsAndSeek(assignedTps, consumer, positionTimeout)

        case AssignWithOffset(assignedOffsets) =>
          checkOverlappingRequests("AssignWithOffset", sender(), assignedOffsets.keySet)
          val previousAssigned = consumer.assignment()
          consumer.assign((assignedOffsets.keys.toSeq ++ previousAssigned.asScala).asJava)
          assignedOffsets.foreach {
            case (tp, offset) =>
              consumer.seek(tp, offset)
          }
          progressTracker.assignedPositions(assignedOffsets.keySet, assignedOffsets)

        case AssignOffsetsForTimes(timestampsToSearch) =>
          checkOverlappingRequests("AssignOffsetsForTimes", sender(), timestampsToSearch.keySet)
          val previousAssigned = consumer.assignment()
          consumer.assign((timestampsToSearch.keys.toSeq ++ previousAssigned.asScala).asJava)
          val topicPartitionToOffsetAndTimestamp =
            consumer.offsetsForTimes(timestampsToSearch.map { case (k, v) => (k, long2Long(v)) }.toMap.asJava,
                                     offsetForTimesTimeout)
          val assignedOffsets = topicPartitionToOffsetAndTimestamp.asScala.filter(_._2 != null).toMap.map {
            case (tp, oat: OffsetAndTimestamp) =>
              val offset = oat.offset()
              val ts = oat.timestamp()
              log.debug("Get offset {} from topic {} with timestamp {}", offset, tp, ts)
              consumer.seek(tp, offset)
              tp -> offset
          }
          progressTracker.assignedPositions(assignedOffsets.keySet, assignedOffsets)

        case Subscribe(topics, rebalanceHandler) =>
          val callback = new RebalanceListenerImpl(rebalanceHandler)
          partitionAssignmentHandler = callback
          consumer.subscribe(topics.toList.asJava, callback)

        case SubscribePattern(pattern, rebalanceHandler) =>
          val callback = new RebalanceListenerImpl(rebalanceHandler)
          partitionAssignmentHandler = callback
          consumer.subscribe(Pattern.compile(pattern), callback)

      }
      scheduleFirstPollTask()
      stageActorsMap = stageActorsMap.updated(consumer.assignment().asScala.toSet, sender())
    } catch {
      case NonFatal(ex) => sendFailure(ex, sender())
    }

  def checkOverlappingRequests(updateType: String, fromStage: ActorRef, topics: Set[TopicPartition]): Unit =
    // check if same topics/partitions have already been requested by someone else,
    // which is an indication that something is wrong, but it might be alright when assignments change.
    if (requests.nonEmpty) requests.foreach {
      case (ref, r) =>
        if (ref != fromStage && r.tps.exists(topics.apply)) {
          log.warning("{} from topic/partition {} already requested by other stage {}", updateType, topics, r.tps)
          ref ! Messages(r.requestId, Iterator.empty)
          requests -= ref
        }
    }

  def stopping: Receive = LoggingReceive.withLabel("stopping") {
    case p: Poll[_, _] =>
      receivePoll(p)
    case _: StopLike =>
    case Terminated(ref) =>
      stageActorsMap = stageActorsMap.filterNot(_._2 == ref)
    case _ @(_: Commit | _: RequestMessages) =>
      sender() ! Status.Failure(StoppingException())
    case msg @ (_: Assign | _: AssignWithOffset | _: Subscribe | _: SubscribePattern) =>
      log.warning("Got unexpected message {} when KafkaConsumerActor is in stopping state", msg)
  }

  override def preStart(): Unit = {
    super.preStart()
    log.debug("Starting {}", self)
    val updateSettings: Future[ConsumerSettings[K, V]] = _settings.enriched
    updateSettings.value match {
      case Some(Success(s)) => applySettings(s)
      case Some(scala.util.Failure(e)) =>
        owner.foreach(_ ! Failure(e))
        throw e
      case None =>
        import akka.pattern.pipe
        implicit val ec: ExecutionContext = context.dispatcher
        context.become(expectSettings)
        updateSettings.pipeTo(self)
    }
  }

  private def applySettings(updatedSettings: ConsumerSettings[K, V]): Unit = {
    this.settings = updatedSettings
    if (settings.connectionCheckerSettings.enable)
      context.actorOf(ConnectionChecker.props(settings.connectionCheckerSettings))
    pollTimeout = settings.pollTimeout.toJava
    offsetForTimesTimeout = settings.getOffsetForTimesTimeout
    positionTimeout = settings.getPositionTimeout
    val progressTrackingFactory: () => ConsumerProgressTracking = () => ensureProgressTracker()
    commitRefreshing = CommitRefreshing(settings.commitRefreshInterval, progressTrackingFactory)
    resetProtection = ConsumerResetProtection(log, settings.resetProtectionSettings, progressTrackingFactory)
    try {
      if (log.isDebugEnabled)
        log.debug(s"Creating Kafka consumer with ${settings.toString}")
      consumer = settings.consumerFactory.apply(settings)
      context.become(regularReceive)
      unstashAll()
    } catch {
      case e: Exception =>
        owner.foreach(_ ! Failure(e))
        throw e
    }
  }

  private def ensureProgressTracker(): ConsumerProgressTracking = {
    if (progressTracker == ConsumerProgressTrackerNoop) {
      progressTracker = new ConsumerProgressTrackerImpl()
    }
    progressTracker
  }

  override def postStop(): Unit = {
    // reply to outstanding requests is important if the actor is restarted
    requests.foreach {
      case (ref, req) =>
        ref ! Messages(req.requestId, Iterator.empty)
    }
    partitionAssignmentHandler.postStop()
    consumer.close(settings.getCloseTimeout)
    super.postStop()
  }

  def scheduleFirstPollTask(): Unit =
    if (!timers.isTimerActive(PollTask)) schedulePollTask()

  def schedulePollTask(): Unit =
    timers.startSingleTimer(PollTask, pollMsg, settings.pollInterval)

  /**
   * Sends an extra `Poll(periodic=false)` request to self.
   * Enqueueing an extra poll via the actor mailbox allows other requests to be handled
   * before the actual poll is executed.
   * With many requestors, e.g. many partitions with `committablePartitionedSource` the
   * performance is much improved by collecting more requests/commits before performing the poll.
   */
  private def requestDelayedPoll(): Unit =
    if (!delayedPollInFlight) {
      delayedPollInFlight = true
      self ! delayedPollMsg
    }

  private def emergencyPoll(): Unit = {
    log.debug("Performing emergency poll")
    commitAndPoll()
  }

  private def receivePoll(p: Poll[_, _]): Unit =
    if (p.target == this) {
      commitAndPoll()
      if (p.periodic)
        schedulePollTask()
      else
        delayedPollInFlight = false
    } else {
      // Message was enqueued before a restart - can be ignored
      log.debug("Ignoring Poll message with stale target ref")
    }

  private def commitAndPoll(): Unit = {
    val refreshOffsets = commitRefreshing.refreshOffsets
    if (refreshOffsets.nonEmpty) {
      log.debug("Refreshing committed offsets: {}", refreshOffsets)
      commit(refreshOffsets, Vector.empty)
    }
    poll()
  }

  def poll(): Unit = {
    try {
      val currentAssignmentsJava = consumer.assignment()
      commitAggregatedOffsets()
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
        val partitionsToFetch: Set[TopicPartition] = requests.values.flatMap(_.tps).toSet
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

    if (stopInProgress && commitsInProgress == 0) {
      log.debug("Stopping")
      context.stop(self)
    }
  }

  private def commitAggregatedOffsets(): Unit = if (commitMaps.nonEmpty) {
    val aggregatedOffsets = aggregateOffsets(commitMaps)
    // commits can occur after the partition has been revoked from the consumer, so ensure that we only attempt to
    // commit partitions that are currently assigned to the consumer. For high volume topics, this can lead to small
    // amounts of replayed data during a rebalance, but for low volume topics we can ensure that consumers never appear
    // 'stuck' because of out-of-order commits from slow consumers.
    val assignedOffsetsToCommit =
      aggregatedOffsets.iterator.filter { case (k, _) => consumer.assignment().contains(k) }.toMap
    progressTracker.commitRequested(assignedOffsetsToCommit)
    val replyTo = commitSenders
    // flush the data before calling `consumer.commitAsync` which might call the callback synchronously
    commitMaps = List.empty
    commitSenders = Vector.empty
    commit(assignedOffsetsToCommit, replyTo)
  }

  private def commit(commitMap: Map[TopicPartition, OffsetAndMetadata], replyTo: Vector[ActorRef]): Unit = {
    commitRefreshing.updateRefreshDeadlines(commitMap.keySet)
    commitsInProgress += 1
    val startTime = System.nanoTime()
    consumer.commitAsync(
      commitMap.asJava,
      new OffsetCommitCallback {
        override def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata],
                                exception: Exception): Unit = {
          def retryCommits(duration: Long, e: Throwable): Unit = {
            log.warning("Kafka commit is to be retried, after={} ms, commitsInProgress={}, cause={}",
                        duration / 1000000L,
                        commitsInProgress,
                        e.toString)
            commitMaps = commitMap.toList ++ commitMaps
            commitSenders = commitSenders ++ replyTo
            requestDelayedPoll()
          }

          // this is invoked on the thread calling consumer.poll which will always be the actor, so it is safe
          val duration = System.nanoTime() - startTime
          commitsInProgress -= 1
          exception match {
            case null =>
              if (duration > settings.commitTimeWarning.toNanos) {
                log.warning("Kafka commit took longer than `commit-time-warning`: {} ms, commitsInProgress={}",
                            duration / 1000000L,
                            commitsInProgress)
              }
              progressTracker.committed(offsets)
              replyTo.foreach(_ ! Done)

            case e @ (_: RebalanceInProgressException | _: TimeoutException | _: CoordinatorLoadInProgressException) =>
              retryCommits(duration, e)
            case e: RetriableCommitFailedException => retryCommits(duration, e.getCause)

            case commitException =>
              log.error("Kafka commit failed after={} ms, commitsInProgress={}, exception={}",
                        duration / 1000000L,
                        commitsInProgress,
                        commitException)
              val failure = Status.Failure(commitException)
              replyTo.foreach(_ ! failure)
          }
        }
      }
    )
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

      val safeRecords = resetProtection.protect(self, rawResult)
      progressTracker.received(safeRecords)

      //send messages to actors
      requests.foreach {
        case (stageActorRef, req) =>
          //gather all messages for ref
          // See https://github.com/akka/alpakka-kafka/issues/978
          // Temporary fix to avoid https://github.com/scala/bug/issues/11807
          // Using `VectorIterator` avoids the error from `ConcatIterator`
          val b = Vector.newBuilder[ConsumerRecord[K, V]]
          req.tps.foreach { tp =>
            val tpMessages = safeRecords.records(tp).asScala
            b ++= tpMessages
          }
          val messages = b.result().iterator
          if (messages.nonEmpty) {
            stageActorRef ! Messages(req.requestId, messages)
            requests -= stageActorRef
          }
      }
    }

  private def sendFailure(exception: Throwable, stageActorRef: ActorRef): Unit = {
    stageActorRef ! Failure(exception)
    stageActorsMap = stageActorsMap.filterNot(_._2 == stageActorRef)
    requests -= stageActorRef
  }

  private def processErrors(exception: Throwable): Unit = {
    val sendTo = (stageActorsMap.values ++ owner).toSet
    log.debug(s"sending failure {} to {}", exception.getClass, sendTo.mkString(","))
    stageActorsMap.values.foreach { stageActorRef =>
      sendFailure(exception, stageActorRef)
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

    case Metadata.GetCommittedOffsets(partitions) =>
      Metadata.CommittedOffsets(
        Try {
          consumer
            .committed(partitions.asJava, settings.getMetadataRequestTimeout)
            .asScala
            .filterNot(_._2 == null)
            .toMap
        }
      )

    case req: Metadata.GetCommittedOffset @nowarn("cat=deprecation") =>
      @nowarn("cat=deprecation") val resp = Metadata.CommittedOffset(
        Try {
          @nowarn("cat=deprecation") val offset = consumer.committed(req.partition, settings.getMetadataRequestTimeout)
          offset
        },
        req.partition
      )
      resp
  }

  private def stopFromMessage(msg: StopLike) = msg match {
    case Stop => sender()
    case StopFromStage(sourceStageId) => s"StageId [$sourceStageId]"
    case other => s"unknown: [$other]"
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
  private[KafkaConsumerActor] sealed trait RebalanceListener
      extends ConsumerRebalanceListener
      with NoSerializationVerificationNeeded {
    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit
    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit

    def postStop(): Unit = ()
  }

  private[KafkaConsumerActor] object RebalanceListener {
    object Empty extends RebalanceListener {
      override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = ()

      override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = ()

      override def postStop(): Unit = ()
    }
  }

  private[KafkaConsumerActor] final class RebalanceListenerImpl(
      partitionAssignmentHandler: PartitionAssignmentHandler
  ) extends RebalanceListener {

    private val restrictedConsumer =
      new RestrictedConsumer(consumer, java.time.Duration.ofNanos(settings.partitionHandlerWarning.*(0.95d).toNanos))
    private val warningDuration = settings.partitionHandlerWarning.toNanos

    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      consumer.pause(partitions)
      val tps = partitions.asScala.toSet
      progressTracker.assignedPositionsAndSeek(tps, consumer, positionTimeout)
      val startTime = System.nanoTime()
      partitionAssignmentHandler.onAssign(tps, restrictedConsumer)
      checkDuration(startTime, "onAssign")
    }

    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      val revokedTps = partitions.asScala.toSet
      val startTime = System.nanoTime()
      updateGroupMetadataSubscribers()
      partitionAssignmentHandler.onRevoke(revokedTps, restrictedConsumer)
      checkDuration(startTime, "onRevoke")
      progressTracker.revoke(revokedTps)
    }

    override def onPartitionsLost(partitions: java.util.Collection[TopicPartition]): Unit = {
      val lostTps = partitions.asScala.toSet
      val startTime = System.nanoTime()
      updateGroupMetadataSubscribers()
      partitionAssignmentHandler.onLost(lostTps, restrictedConsumer)
      checkDuration(startTime, "onLost")
      progressTracker.revoke(lostTps)
    }

    override def postStop(): Unit = {
      val currentTps = consumer.assignment()
      consumer.pause(currentTps)
      val startTime = System.nanoTime()
      updateGroupMetadataSubscribers()
      partitionAssignmentHandler.onStop(currentTps.asScala.toSet, restrictedConsumer)
      checkDuration(startTime, "onStop")
    }

    private def checkDuration(startTime: Long, method: String): Unit = {
      val duration = System.nanoTime() - startTime
      if (duration > warningDuration) {
        log.warning("Partition assignment handler `{}` took longer than `partition-handler-warning`: {} ms",
                    method,
                    duration / 1000000L)
      }
    }

    def updateGroupMetadataSubscribers(): Unit = {
      if (metadataSubscribers.nonEmpty) {
        sendConsumerGroupMetadataToSubscribers()
      }
    }
  }

}
