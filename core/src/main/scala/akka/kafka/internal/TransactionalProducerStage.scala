/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffsetCommittedMarker}
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.internal.DeferredProducer._
import akka.kafka.internal.ProducerStage.ProducerCompletionState
import akka.kafka.internal.TransactionalProducerStage.TransactionBatch.FirstAndHighestOffsets
import akka.kafka.{ConsumerMessage, ProducerSettings}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

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
private[kafka] object TransactionalProducerStage {
  private val tpFirstEndOffsetFormat = "%-64s%-25s%s\n"

  object TransactionBatch {
    def empty: TransactionBatch = new EmptyTransactionBatch()

    final case class FirstAndHighestOffsets(first: Long, highest: Long)
  }

  private[kafka] sealed trait TransactionBatch {
    def updated(partitionOffset: PartitionOffsetCommittedMarker): NonemptyTransactionBatch
    def transactionAborted(): Future[Unit]
  }

  final class EmptyTransactionBatch extends TransactionBatch {
    override def updated(partitionOffset: PartitionOffsetCommittedMarker): NonemptyTransactionBatch =
      new NonemptyTransactionBatch(partitionOffset)

    override def transactionAborted(): Future[Unit] = Future.successful(Done)

    override def toString: String = s"(empty transaction)"
  }

  final class NonemptyTransactionBatch(head: PartitionOffsetCommittedMarker,
                                       tail: Map[GroupTopicPartition, FirstAndHighestOffsets] = Map.empty)
      extends TransactionBatch {

    private val withHighest = tail.get(head.key) match {
      // There is no guarantee that offsets adding callbacks will be called in any particular order.
      // Decreasing an offset stored for the KTP would mean possible data duplication.
      // Since `awaitingConfirmation` counter guarantees that all writes finished, we can safely assume
      // that all data up to maximal offsets has been written to Kafka.
      case Some(FirstAndHighestOffsets(first, previousHighest)) if previousHighest < head.offset =>
        FirstAndHighestOffsets(first, head.offset)
      case Some(last @ FirstAndHighestOffsets(_, _)) => last
      case None => FirstAndHighestOffsets(head.offset, head.offset)
    }

    private[internal] val offsets = tail + (head.key -> withHighest)

    def group: String = head.key.groupId
    def committedMarker: CommittedMarker = head.committedMarker

    def offsetMap(): Map[TopicPartition, OffsetAndMetadata] = offsets.map {
      case (gtp, FirstAndHighestOffsets(_, offset)) =>
        new TopicPartition(gtp.topic, gtp.partition) -> new OffsetAndMetadata(offset + 1)
    }

    def internalCommit(): Future[Done] =
      committedMarker.committed(offsetMap())

    override def transactionAborted(): Future[Unit] =
      committedMarker.onTransactionAborted.success(()).future

    override def updated(partitionOffset: PartitionOffsetCommittedMarker): NonemptyTransactionBatch = {
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

    override def toString: String = {
      val header = tpFirstEndOffsetFormat.format("TopicPartition", "First Observed Offset", "End Offset")
      val rows = offsets.map {
        case (gtp, FirstAndHighestOffsets(first, highest)) =>
          tpFirstEndOffsetFormat.format(gtp.topicPartition.toString, first, highest)
      }
      "\n" + header + rows.mkString
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

  private var firstMessage: Option[Envelope[K, V, P]] = None

  override protected def logSource: Class[_] = classOf[TransactionalProducerStage[_, _, _]]

  // we need to peek at the first message to generate the producer transactional id for partitioned sources
  override def preStart(): Unit = resumeDemand()

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
      msg.passThrough match {
        case partitionOffset: PartitionOffsetCommittedMarker =>
          partitionOffset.committedMarker.onFirstMessageReceived.success(())
        case _ => ()
      }
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
      maybeCommitTransaction()
    }

  private def maybeCommitTransaction(beginNewTransaction: Boolean = true,
                                     abortEmptyTransactionOnComplete: Boolean = false): Unit = {
    val awaitingConf = awaitingConfirmationValue
    batchOffsets match {
      case batch: NonemptyTransactionBatch if awaitingConf == 0 =>
        commitTransaction(batch, beginNewTransaction)
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
   * When using partitioned sources we extract the transactional id, group id, and topic partition information from
   * the first message in order to define a `transacitonal.id` before constructing the [[org.apache.kafka.clients.producer.KafkaProducer]]
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
        // initiate async async producer request _after_ first message is stashed in case future eagerly resolves
        // instead of asynccallback
        resolveProducer(generatedTransactionalConfig(msg))
        // suspend demand after we receive the first message until the producer is assigned
        suspendDemand()
        false
      case AsyncCreateRequestSent =>
        throw new IllegalStateException(
          s"Should never receive new messages while in producer assignment state '$AsyncCreateRequestSent'"
        )
    }

  private def generatedTransactionalConfig(msg: Envelope[K, V, P]): ProducerSettings[K, V] = {
    val txId = msg.passThrough match {
      case committedMarker: PartitionOffsetCommittedMarker if committedMarker.fromPartitionedSource =>
        val gtp = committedMarker.key
        val txId = s"$transactionalId-${gtp.groupId}-${gtp.topic}-${gtp.partition}"
        log.debug("Generated transactional id from partitioned source '{}'", txId)
        txId
      case _ => transactionalId
    }

    stage.settings.withProperties(
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> txId,
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString
    )
  }

  override protected def postSend(msg: Envelope[K, V, P]): Unit = msg.passThrough match {
    case o: ConsumerMessage.PartitionOffsetCommittedMarker =>
      batchOffsets = batchOffsets.updated(o)
  }

  override def onCompletionSuccess(): Unit = {
    log.debug("Committing final transaction before shutdown")
    cancelTimer(commitSchedulerKey)
    maybeCommitTransaction(beginNewTransaction = false, abortEmptyTransactionOnComplete = true)
    super.onCompletionSuccess()
  }

  override def onCompletionFailure(ex: Throwable): Unit = {
    cancelTimer(commitSchedulerKey)
    abortTransaction("Stage failure")

    batchOffsets
      .transactionAborted()
      .onComplete {
        case Success(_) =>
          log.debug("Successfully sent source stage failure")
          onCommitFailedAckCb.invoke(ex)
        case Failure(commitEx) if NonFatal(commitEx) =>
          log.error(commitEx, "Failed to send source stage failure")
          onCommitFailedAckCb.invoke(commitEx)
      }(materializer.executionContext)
  }

  private def commitTransaction(batch: NonemptyTransactionBatch, beginNewTransaction: Boolean): Unit = {
    val group = batch.group
    log.debug("Committing transaction for transactional id '{}' consumer group '{}' with offsets: {}",
              transactionalId,
              group,
              batch.toString)
    val offsetMap = batch.offsetMap()
    producer.sendOffsetsToTransaction(offsetMap.asJava, group)
    producer.commitTransaction()
    log.debug("Committed transaction for transactional id '{}' consumer group '{}' with offsets: {}",
              transactionalId,
              group,
              batch.toString)
    batchOffsets = TransactionBatch.empty

    batch
      .internalCommit()
      .onComplete { _ =>
        onInternalCommitAckCb.invoke(())
      }(materializer.executionContext)
    if (beginNewTransaction) {
      beginTransaction()
      resumeDemand()
    }
  }

  private val onCommitFailedAckCb: AsyncCallback[Throwable] = {
    getAsyncCallback[Throwable](
      t => super.onCompletionFailure(t)
    )
  }

  private val onInternalCommitAckCb: AsyncCallback[Unit] = {
    getAsyncCallback[Unit](
      _ => scheduleOnce(commitSchedulerKey, producerSettings.eosCommitInterval)
    )
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
    log.debug("Aborting transaction with reason: {}, offsets: {}", reason, batchOffsets.toString)
    if (producerAssignmentLifecycle == Assigned) producer.abortTransaction()
  }
}
