/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.Done
import akka.actor.Terminated
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffsetCommittedMarker}
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.internal.DeferredProducer._
import akka.kafka.internal.ProducerStage.ProducerCompletionState
import akka.kafka.{ConsumerMessage, ProducerSettings}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape}
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] final class TransactionalProducerStage[K, V, P](
    val settings: ProducerSettings[K, V],
    transactionalId: String
) extends GraphStage[FlowShape[Envelope[K, V, P], Future[Results[K, V, P]]]]
    with ProducerStage[K, V, P, Envelope[K, V, P], Results[K, V, P]] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TransactionalProducerStageLogic(this, transactionalId, inheritedAttributes)
}

/** Internal API */
private object TransactionalProducerStage {
  object TransactionBatch {
    def empty: TransactionBatch = new EmptyTransactionBatch()
  }

  private[kafka] sealed trait TransactionBatch {
    def updated(partitionOffset: PartitionOffsetCommittedMarker): TransactionBatch
    def committingFailed(): Unit
  }

  final class EmptyTransactionBatch extends TransactionBatch {
    override def updated(partitionOffset: PartitionOffsetCommittedMarker): TransactionBatch =
      new NonemptyTransactionBatch(partitionOffset)

    override def committingFailed(): Unit = {}
  }

  final class NonemptyTransactionBatch(val head: PartitionOffsetCommittedMarker,
                                       tail: Map[GroupTopicPartition, Long] = Map[GroupTopicPartition, Long]())
      extends TransactionBatch {
    // There is no guarantee that offsets adding callbacks will be called in any particular order.
    // Decreasing an offset stored for the KTP would mean possible data duplication.
    // Since `awaitingConfirmation` counter guarantees that all writes finished, we can safely assume
    // that all all data up to maximal offsets has been wrote to Kafka.
    private val previousHighest = tail.getOrElse(head.key, -1L)
    private[internal] val offsets = tail + (head.key -> head.offset.max(previousHighest))

    def group: String = head.key.groupId
    def committedMarker: CommittedMarker = head.committedMarker

    def offsetMap(): Map[TopicPartition, OffsetAndMetadata] = offsets.map {
      case (gtp, offset) => new TopicPartition(gtp.topic, gtp.partition) -> new OffsetAndMetadata(offset + 1)
    }

    def internalCommit(): Future[Done] =
      committedMarker.committed(offsetMap())

    override def committingFailed(): Unit =
      committedMarker.failed()

    override def updated(partitionOffset: PartitionOffsetCommittedMarker): TransactionBatch = {
      require(
        group == partitionOffset.key.groupId,
        s"Transaction batch must contain messages from exactly 1 consumer group. $group != ${partitionOffset.key.groupId}"
      )
      require(
        this.committedMarker == partitionOffset.committedMarker,
        "Transaction batch must contain messages from a single source"
      )
      new NonemptyTransactionBatch(partitionOffset, offsets)
    }
  }

}

/**
 * Internal API.
 *
 * Transaction (Exactly-Once) Producer State Logic
 */
private final class TransactionalProducerStageLogic[K, V, P](
    stage: TransactionalProducerStage[K, V, P],
    transactionalId: String,
    inheritedAttributes: Attributes
) extends DefaultProducerStageLogic[K, V, P, Envelope[K, V, P], Results[K, V, P]](stage, inheritedAttributes)
    with StageIdLogging
    with ProducerCompletionState {

  import TransactionalProducerStage._

  private val commitSchedulerKey = "commit"
  private val messageDrainInterval = 10.milliseconds
  private var batchOffsets = TransactionBatch.empty
  private var demandSuspended = false
  private var latestSeenConsumerGroupMetadata: ConsumerGroupMetadata = null

  private var firstMessage: Option[Envelope[K, V, P]] = None

  private val onInternalCommitAckCb: Try[Done] => Unit =
    getAsyncCallback[Try[Done]] { maybeDone =>
      maybeDone match {
        case Failure(ex) => log.debug("Internal commit failed: {}", ex)
        case _ =>
      }
      scheduleOnce(commitSchedulerKey, producerSettings.eosCommitInterval)
    }.invoke _

  override protected def logSource: Class[_] = classOf[TransactionalProducerStage[_, _, _]]

  override def preStart(): Unit = {
    resumeDemand()
    getStageActor {
      case (_, newMetadata: ConsumerGroupMetadata) => latestSeenConsumerGroupMetadata = newMetadata
      case (_, Terminated(_)) =>
        abortTransaction("Consumer actor stopped")
        failStage(new RuntimeException("Consumer actor stopped, no transactions can be committed without consumer"))
    }
  }

  override protected def producerAssigned(): Unit = {
    producingInHandler()
    initTransactions()
    beginTransaction()
    produceFirstMessage()
    resumeDemand()
    scheduleOnce(commitSchedulerKey, producerSettings.eosCommitInterval)
  }

  private def produceFirstMessage(): Unit = firstMessage match {
    case Some(msg) =>
      produce(msg)
      firstMessage = None
    case _ =>
      throw new IllegalStateException("Should never attempt to produce first message if it does not exist.")
  }

  override protected def resumeDemand(tryToPull: Boolean = true): Unit = {
    super.resumeDemand(tryToPull)
    demandSuspended = false
  }

  override protected def suspendDemand(): Unit = {
    if (!demandSuspended) super.suspendDemand()
    demandSuspended = true
  }

  override protected def initialInHandler(): Unit =
    setHandler(stage.in, new DefaultInHandler {
      override def onPush(): Unit = parseFirstMessage(grab(stage.in))
    })

  override protected def onTimer(timerKey: Any): Unit =
    if (timerKey == commitSchedulerKey) {
      commitTransaction()
    }

  private def commitTransaction(beginNewTransaction: Boolean = true,
                                abortEmptyTransactionOnComplete: Boolean = false): Unit = {
    val awaitingConf = awaitingConfirmationValue
    batchOffsets match {
      case batch: NonemptyTransactionBatch if awaitingConf == 0 =>
        try {
          log.debug("Committing transaction for transactional id '{}' consumer group '{}' with offsets: {}",
                    transactionalId,
                    latestSeenConsumerGroupMetadata,
                    batch.offsets)
          val offsetMap = batch.offsetMap()
          producer.sendOffsetsToTransaction(offsetMap.asJava, latestSeenConsumerGroupMetadata)
          producer.commitTransaction()
          log.debug("Committed transaction for transactional id '{}' consumer group '{}' with offsets: {}",
                    transactionalId,
                    latestSeenConsumerGroupMetadata,
                    batch.offsets)
          batchOffsets = TransactionBatch.empty
          batch
            .internalCommit()
            .onComplete(onInternalCommitAckCb)(ExecutionContexts.parasitic)
        } catch {
          case e: ProducerFencedException =>
            log.debug(s"Producer fenced: $e")
            failStage(e)
          case e: KafkaException =>
            abortTransaction(s"Kafka threw exception: $e")
            batch
              .committingFailed()
        }
        if (beginNewTransaction) {
          beginTransaction()
          resumeDemand()
        }

      case _: EmptyTransactionBatch if awaitingConf == 0 && abortEmptyTransactionOnComplete =>
        abortTransaction("Transaction is empty and stage is completing")
      case _ if awaitingConf > 0 =>
        suspendDemand()
        scheduleOnce(commitSchedulerKey, messageDrainInterval)
      case _ =>
        scheduleOnce(commitSchedulerKey, producerSettings.eosCommitInterval)
    }
  }

  /**
   * When using partitioned sources we need to have access to the ConsumerGroupMetadata from the Kafka consumer which
   * is single thread-only, so we can't access it directly, instead we need to subscribe to get periodic updates of
   * the latest consumer group metadata, we can only do that once the first message arrives
   */
  private def parseFirstMessage(msg: Envelope[K, V, P]): Boolean =
    producerAssignmentLifecycle match {
      case Assigned => true
      case Unassigned if firstMessage.nonEmpty =>
        // this should never happen because demand should be suspended until the producer is assigned
        throw new IllegalStateException("Cannot reapply first message")
      case Unassigned =>
        // stash the first message so it can be sent after the producer is assigned
        firstMessage = Some(msg)

        // Set up getting periodic group metadata as those can't be safely async-request fetched
        // during partition rebalance drain which blocks the consumer actor
        msg.passThrough match {
          case o: ConsumerMessage.PartitionOffsetCommittedMarker =>
            o.consumerActor ! KafkaConsumerActor.Internal.SubscribeToGroupMetaData(stageActor.ref)
            // We depend on consumer actor now so couple lifecycles
            stageActor.watch(o.consumerActor)
          case _ =>
        }
        // initiate async async producer request _after_ first message is stashed in case future eagerly resolves
        // instead of asynccallback
        resolveProducer(generatedTransactionalConfig)
        // suspend demand after we receive the first message until the producer is assigned
        suspendDemand()
        false
      case AsyncCreateRequestSent =>
        throw new IllegalStateException(
          s"Should never receive new messages while in producer assignment state '$AsyncCreateRequestSent'"
        )
    }

  private def generatedTransactionalConfig: ProducerSettings[K, V] = {
    stage.settings.withProperties(
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId,
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString,
      // KIP-447: "We shall set `transaction.timout.ms` default to 10000 ms (10 seconds) on Kafka Streams. For non-stream users,
      //           we highly recommend you to do the same if you want to use the new semantics."
      ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> 10000.toString
    )
  }

  override protected def postSend(msg: Envelope[K, V, P]): Unit = msg.passThrough match {
    case o: ConsumerMessage.PartitionOffsetCommittedMarker => batchOffsets = batchOffsets.updated(o)
    case _ =>
  }

  override def onCompletionSuccess(): Unit = {
    log.debug("Committing final transaction before shutdown")
    cancelTimer(commitSchedulerKey)
    commitTransaction(beginNewTransaction = false, abortEmptyTransactionOnComplete = true)
    super.onCompletionSuccess()
  }

  override def onCompletionFailure(ex: Throwable): Unit = {
    abortTransaction(s"Stage failure ($ex)")
    batchOffsets.committingFailed()
    super.onCompletionFailure(ex)
  }

  private def initTransactions(): Unit = {
    log.debug("Initializing transactions")
    producer.initTransactions()
  }

  private def beginTransaction(): Unit = {
    log.debug("Beginning new transaction")
    producer.beginTransaction()
  }

  private def abortTransaction(reason: String): Unit = {
    log.debug("Aborting transaction: {}", reason)
    if (producerAssignmentLifecycle == Assigned) producer.abortTransaction()
    batchOffsets.committingFailed()
  }

}
