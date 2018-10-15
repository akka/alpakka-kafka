/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
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
import akka.event.LoggingReceive
import akka.kafka.KafkaConsumerActor.StoppingException
import akka.kafka.{ConsumerSettings, Metadata}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.{NoStackTrace, NonFatal}

object KafkaConsumerActor {

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
    private[KafkaConsumerActor] final case class PartitionAssigned(
        assignedOffsets: Map[TopicPartition, Long]
    ) extends DeadLetterSuppression
        with NoSerializationVerificationNeeded

    private[KafkaConsumerActor] final case class PartitionRevoked(
        revokedTps: Set[TopicPartition]
    ) extends DeadLetterSuppression
        with NoSerializationVerificationNeeded

    private[KafkaConsumerActor] case object PollTask

    private val number = new AtomicInteger()
    def nextNumber(): Int =
      number.incrementAndGet()

    private[KafkaConsumerActor] class NoPollResult extends RuntimeException with NoStackTrace
  }

  case class ListenerCallbacks(onAssign: Set[TopicPartition] => Unit, onRevoke: Set[TopicPartition] => Unit)
      extends NoSerializationVerificationNeeded

  /**
   * Copied from the implemented interface:
   *
   * These methods will be called after the partition re-assignment completes and before the
   * consumer starts fetching data, and only as the result of a `poll` call.
   *
   * It is guaranteed that all the processes in a consumer group will execute their
   * `onPartitionsRevoked` callback before any instance executes its
   * `onPartitionsAssigned` callback.
   */
  private class WrappedAutoPausedListener(consumer: Consumer[_, _],
                                          consumerActor: ActorRef,
                                          positionTimeout: java.time.Duration,
                                          listener: ListenerCallbacks)
      extends ConsumerRebalanceListener
      with NoSerializationVerificationNeeded {
    import KafkaConsumerActor.Internal._
    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      consumer.pause(partitions)
      val tps = partitions.asScala.toSet
      val assignedOffsets = tps.map(tp => tp -> consumer.position(tp, positionTimeout)).toMap
      consumerActor ! PartitionAssigned(assignedOffsets)
      listener.onAssign(tps)
    }

    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      val revokedTps = partitions.asScala.toSet
      listener.onRevoke(revokedTps)
      consumerActor ! PartitionRevoked(revokedTps)
    }
  }
}

class KafkaConsumerActor[K, V](settings: ConsumerSettings[K, V]) extends Actor with ActorLogging with Timers {
  import KafkaConsumerActor.Internal._
  import KafkaConsumerActor._

  val pollMsg = Poll(this, periodic = true)
  val delayedPollMsg = Poll(this, periodic = false)
  def pollTimeout() = settings.pollTimeout
  def pollInterval() = settings.pollInterval

  /** Limits the blocking on offsetForTimes */
  val offsetForTimesTimeout = settings.getOffsetForTimesTimeout

  /** Limits the blocking on position in [[WrappedAutoPausedListener]] */
  val positionTimeout = settings.getPositionTimeout

  var requests = Map.empty[ActorRef, RequestMessages]
  var requestors = Set.empty[ActorRef]
  var consumer: Consumer[K, V] = _
  var subscriptions = Set.empty[SubscriptionRequest]
  var commitsInProgress = 0
  var commitRequestedOffsets = Map.empty[TopicPartition, OffsetAndMetadata]
  var committedOffsets = Map.empty[TopicPartition, OffsetAndMetadata]
  var commitRefreshDeadline: Option[Deadline] = None
  var initialPoll = true
  var wakeups = 0
  var stopInProgress = false
  var delayedPollInFlight = false

  def receive: Receive = LoggingReceive {
    case Assign(tps) =>
      scheduleFirstPollTask()
      checkOverlappingRequests("Assign", sender(), tps)
      val previousAssigned = consumer.assignment()
      consumer.assign((tps.toSeq ++ previousAssigned.asScala).asJava)
      val assignedOffsets = tps.map(tp => tp -> consumer.position(tp)).toMap
      self ! PartitionAssigned(assignedOffsets)

    case AssignWithOffset(assignedOffsets) =>
      scheduleFirstPollTask()
      checkOverlappingRequests("AssignWithOffset", sender(), assignedOffsets.keySet)
      val previousAssigned = consumer.assignment()
      consumer.assign((assignedOffsets.keys.toSeq ++ previousAssigned.asScala).asJava)
      assignedOffsets.foreach {
        case (tp, offset) =>
          consumer.seek(tp, offset)
      }
      self ! PartitionAssigned(assignedOffsets)

    case AssignOffsetsForTimes(timestampsToSearch) =>
      scheduleFirstPollTask()
      checkOverlappingRequests("AssignOffsetsForTimes", sender(), timestampsToSearch.keySet)
      val previousAssigned = consumer.assignment()
      consumer.assign((timestampsToSearch.keys.toSeq ++ previousAssigned.asScala).asJava)
      val topicPartitionToOffsetAndTimestamp =
        consumer.offsetsForTimes(timestampsToSearch.mapValues(long2Long).asJava, offsetForTimesTimeout)
      val assignedOffsets = topicPartitionToOffsetAndTimestamp.asScala.filter(_._2 != null).toMap.map {
        case (tp, oat: OffsetAndTimestamp) =>
          val offset = oat.offset()
          val ts = oat.timestamp()
          log.debug("Get offset {} from topic {} with timestamp {}", offset, tp, ts)
          consumer.seek(tp, offset)
          tp -> offset
      }
      self ! PartitionAssigned(assignedOffsets)

    case Commit(offsets) =>
      commitRequestedOffsets ++= offsets
      commit(offsets, sender())

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

    case PartitionAssigned(assignedOffsets) =>
      commitRequestedOffsets ++= assignedOffsets.map {
        case (partition, offset) =>
          partition -> commitRequestedOffsets.getOrElse(partition, new OffsetAndMetadata(offset))
      }
      committedOffsets ++= assignedOffsets.map {
        case (partition, offset) =>
          partition -> committedOffsets.getOrElse(partition, new OffsetAndMetadata(offset))
      }
      commitRefreshDeadline = nextCommitRefreshDeadline()

    case PartitionRevoked(revokedTps) =>
      commitRequestedOffsets --= revokedTps
      committedOffsets --= revokedTps

    case Committed(offsets) =>
      committedOffsets ++= offsets

    case Stop =>
      if (commitsInProgress == 0) {
        log.debug("Received Stop from {}, stopping", sender())
        context.stop(self)
      } else {
        log.debug("Received Stop from {}, waiting for commitsInProgress={}", sender(), commitsInProgress)
        stopInProgress = true
        context.become(stopping)
      }

    case RequestMetrics =>
      val unmodifiableYetMutableMetrics: java.util.Map[MetricName, _ <: Metric] = consumer.metrics()
      sender() ! ConsumerMetrics(unmodifiableYetMutableMetrics.asScala.toMap)

    case Terminated(ref) =>
      requests -= ref
      requestors -= ref

    case req: Metadata.Request =>
      sender ! handleMetadataRequest(req)
  }

  def handleSubscription(subscription: SubscriptionRequest): Unit = {
    scheduleFirstPollTask()

    subscription match {
      case Subscribe(topics, listener) =>
        consumer.subscribe(topics.toList.asJava,
                           new WrappedAutoPausedListener(consumer, self, positionTimeout, listener))
      case SubscribePattern(pattern, listener) =>
        consumer.subscribe(Pattern.compile(pattern),
                           new WrappedAutoPausedListener(consumer, self, positionTimeout, listener))
    }
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

    consumer = settings.createKafkaConsumer()
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
    timers.startSingleTimer(PollTask, pollMsg, pollInterval())

  private def receivePoll(p: Poll[_, _]): Unit =
    if (p.target == this) {
      if (commitRefreshDeadline.exists(_.isOverdue())) {
        val refreshOffsets = committedOffsets.filter {
          case (tp, offset) =>
            commitRequestedOffsets.get(tp).contains(offset)
        }
        log.debug("Refreshing committed offsets: {}", refreshOffsets)
        commit(refreshOffsets, context.system.deadLetters)
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
    val wakeupTask = context.system.scheduler.scheduleOnce(settings.wakeupTimeout) {
      log.warning(
        "KafkaConsumer poll has exceeded wake up timeout ({}). Waking up consumer to avoid thread starvation.",
        settings.wakeupTimeout.toCoarsest
      )
      if (settings.wakeupDebug && wakeups > settings.maxWakeups / 2) {
        val stacks =
          Thread.getAllStackTraces.asScala.map { case (k, v) => s"$k\n ${v.mkString("\n")}" }.mkString("\n\n")
        log.warning(
          "Wake up has been triggered {} times (See setting akka.kafka.consumer.wakeup-debug). Dumping stacks: {}",
          wakeups,
          stacks
        )
      }
      consumer.wakeup()
    }(context.system.dispatcher)
    val currentAssignmentsJava = consumer.assignment()

    def tryPoll(timeout: Long): ConsumerRecords[K, V] =
      try {
        val records = consumer.poll(timeout)
        initialPoll = false
        wakeups = 0
        records
      } catch {
        case w: WakeupException =>
          wakeups = wakeups + 1
          if (wakeups == settings.maxWakeups) {
            log.error(
              "WakeupException limit exceeded during poll({}), stopping (max-wakeups = {}, wakeup-timeout = {}).",
              timeout,
              settings.maxWakeups,
              settings.wakeupTimeout.toCoarsest
            )
            context.stop(self)
          } else {
            if (log.isWarningEnabled && wakeups > settings.maxWakeups / 2) {
              log.warning(
                "Consumer poll({}) interrupted with WakeupException (#{} of max-wakeups = {}, wakeup-timeout = {}).",
                timeout,
                wakeups,
                settings.maxWakeups,
                settings.wakeupTimeout.toCoarsest
              )
              if (initialPoll) {
                log.error(
                  "Initial consumer poll({}) with bootstrap servers {} did not succeed, still trying",
                  timeout,
                  settings.properties.getOrElse(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "not set")
                )
              }
            }

            // If the current consumer is using group assignment (i.e. subscriptions is non empty) the wakeup might
            // have prevented the re-balance callbacks to be called leaving the Source in an inconsistent state w.r.t
            // assigned TopicPartitions. In order to reconcile the state we manually call callbacks for all added/remove
            // TopicPartition assignments aligning the Source's state the consumer's.
            // We are safe to perform the operation here since the poll() thread has been aborted by the wakeup call
            // and there are no other threads are using the consumer.
            // Note: in case of manual partition assignment this is not needed since rebalance doesn't take place.
            if (subscriptions.nonEmpty) {
              val newAssignments = consumer.assignment().asScala
              reconcileAssignments(currentAssignmentsJava.asScala.toSet, newAssignments.toSet)
            }
          }
          throw new NoPollResult
        case e: org.apache.kafka.common.errors.SerializationException =>
          throw e
        case NonFatal(e) =>
          log.error(e, "Exception when polling from consumer: {}", e.toString)
          context.stop(self)
          throw new NoPollResult
      }

    try {
      if (requests.isEmpty) {
        // no outstanding requests so we don't expect any messages back, but we should anyway
        // drive the KafkaConsumer by polling

        def checkNoResult(rawResult: ConsumerRecords[K, V]): Unit =
          if (!rawResult.isEmpty)
            throw new IllegalStateException(s"Got ${rawResult.count} unexpected messages")

        consumer.pause(currentAssignmentsJava)
        checkNoResult(tryPoll(0))

        // For commits we try to avoid blocking poll because a commit normally succeeds after a few
        // poll(0). Using poll(1) will always block for 1 ms, since there are no messages.
        // Therefore we do 10 poll(0) with short 10 μs delay followed by 1 poll(1).
        // If it's still not completed it will be tried again after the scheduled Poll.
        var i = 10
        while (i > 0 && commitsInProgress > 0) {
          LockSupport.parkNanos(10 * 1000)
          val pollTimeout = if (i == 1) 1L else 0L
          checkNoResult(tryPoll(pollTimeout))
          i -= 1
        }
      } else {
        // resume partitions to fetch
        val partitionsToFetch: Set[TopicPartition] = requests.values.flatMap(_.topics)(collection.breakOut)
        val (resumeThese, pauseThese) = currentAssignmentsJava.asScala.partition(partitionsToFetch.contains)
        consumer.pause(pauseThese.asJava)
        consumer.resume(resumeThese.asJava)
        processResult(partitionsToFetch, tryPoll(pollTimeout().toMillis))
      }
    } catch {
      case e: org.apache.kafka.common.errors.SerializationException =>
        processErrors(e)
      case _: NoPollResult => // already handled, just proceed
    } finally wakeupTask.cancel()

    if (stopInProgress && commitsInProgress == 0) {
      log.debug("Stopping")
      context.stop(self)
    }
  }

  //TODO can't be re-created deterministically so should be pulled out and tested
  private def reconcileAssignments(currentAssignments: Set[TopicPartition],
                                   newAssignments: Set[TopicPartition]): Unit = {

    val revokedAssignmentsByTopic = (currentAssignments -- newAssignments).groupBy(_.topic())
    val addedAssignmentsByTopic = (newAssignments -- currentAssignments).groupBy(_.topic())

    if (settings.wakeupDebug) {
      log.info(
        "Reconciliation has found revoked assignments: {} added assignments: {}. Current subscriptions: {}",
        revokedAssignmentsByTopic,
        addedAssignmentsByTopic,
        subscriptions
      )
    }

    subscriptions.foreach {
      case Subscribe(topics, listener) =>
        topics.foreach { topic =>
          val removedTopicAssignments = revokedAssignmentsByTopic.getOrElse(topic, Set.empty)
          if (removedTopicAssignments.nonEmpty) listener.onRevoke(removedTopicAssignments)

          val addedTopicAssignments = addedAssignmentsByTopic.getOrElse(topic, Set.empty)
          if (addedTopicAssignments.nonEmpty) listener.onAssign(addedTopicAssignments)
        }

      case SubscribePattern(pattern: String, listener) =>
        val ptr = Pattern.compile(pattern)
        def filterByPattern(tpm: Map[String, Set[TopicPartition]]): Set[TopicPartition] =
          tpm.flatMap {
            case (topic, tps) if ptr.matcher(topic).matches() => tps
            case _ => Set.empty[TopicPartition]
          }.toSet

        val revokedAssignments = filterByPattern(revokedAssignmentsByTopic)
        if (revokedAssignments.nonEmpty) listener.onRevoke(revokedAssignments)

        val addedAssignments = filterByPattern(addedAssignmentsByTopic)
        if (addedAssignments.nonEmpty) listener.onAssign(addedAssignments)
    }
  }

  private def nextCommitRefreshDeadline(): Option[Deadline] = settings.commitRefreshInterval match {
    case finite: FiniteDuration => Some(finite.fromNow)
    case infinite => None
  }

  private def commit(commitMap: Map[TopicPartition, OffsetAndMetadata], reply: ActorRef): Unit = {
    commitRefreshDeadline = nextCommitRefreshDeadline()
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
          if (exception != null) reply ! Status.Failure(exception)
          else {
            val committed = Committed(offsets.asScala.toMap)
            self ! committed
            reply ! committed
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

  private def processErrors(exception: Exception): Unit = {
    val involvedStageActors = requests.keys
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
      Metadata.CommittedOffset(Try {
        consumer.committed(partition, settings.getMetadataRequestTimeout)
      })
  }
}
