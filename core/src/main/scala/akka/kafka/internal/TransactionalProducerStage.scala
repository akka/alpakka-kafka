/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.Done
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffsetCommittedMarker}
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.internal.ProducerStage.{MessageCallback, ProducerCompletionState}
import akka.kafka.{ConsumerMessage, ProducerSettings}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] final class TransactionalProducerStage[K, V, P](
    val settings: ProducerSettings[K, V]
) extends GraphStage[FlowShape[Envelope[K, V, P], Future[Results[K, V, P]]]]
    with ProducerStage[K, V, P, Envelope[K, V, P], Results[K, V, P]] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TransactionalProducerStageLogic(this, inheritedAttributes)
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

  final class NonemptyTransactionBatch(head: PartitionOffsetCommittedMarker,
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
    inheritedAttributes: Attributes
) extends DefaultProducerStageLogic[K, V, P, Envelope[K, V, P], Results[K, V, P]](stage, inheritedAttributes)
    with StageLogging
    with MessageCallback[K, V, P]
    with ProducerCompletionState {

  import TransactionalProducerStage._

  private val commitSchedulerKey = "commit"
  private val messageDrainInterval = 10.milliseconds

  private var batchOffsets = TransactionBatch.empty

  private var demandSuspended = false

  override protected def logSource: Class[_] = classOf[TransactionalProducerStage[_, _, _]]

  override def preStart(): Unit = super.preStart()

  override protected def producerAssigned(): Unit = {
    initTransactions()
    beginTransaction()
    resumeDemand()
    scheduleOnce(commitSchedulerKey, producerSettings.eosCommitInterval)
  }

  // suspend demand until a Producer has been created
  suspendDemand()

  override protected def resumeDemand(tryToPull: Boolean = true): Unit = {
    super.resumeDemand(tryToPull)
    demandSuspended = false
  }

  override protected def suspendDemand(): Unit = {
    if (!demandSuspended) super.suspendDemand()
    demandSuspended = true
  }

  override protected def onTimer(timerKey: Any): Unit =
    if (timerKey == commitSchedulerKey) {
      maybeCommitTransaction()
    }

  private def maybeCommitTransaction(beginNewTransaction: Boolean = true,
                                     abortEmptyTransactionOnComplete: Boolean = false): Unit = {
    val awaitingConf = awaitingConfirmation.get
    batchOffsets match {
      case batch: NonemptyTransactionBatch if awaitingConf == 0 =>
        commitTransaction(batch, beginNewTransaction)
      case _: EmptyTransactionBatch if awaitingConf == 0 && abortEmptyTransactionOnComplete =>
        log.debug("Aborting empty transaction because we're completing.")
        abortTransaction()
      case _ if awaitingConf > 0 =>
        suspendDemand()
        scheduleOnce(commitSchedulerKey, messageDrainInterval)
      case _ =>
        scheduleOnce(commitSchedulerKey, producerSettings.eosCommitInterval)
    }
  }

  override def postSend(msg: Envelope[K, V, P]): Unit = msg.passThrough match {
    case o: ConsumerMessage.PartitionOffsetCommittedMarker => batchOffsets = batchOffsets.updated(o)
  }

  override def onCompletionSuccess(): Unit = {
    log.debug("Committing final transaction before shutdown")
    cancelTimer(commitSchedulerKey)
    maybeCommitTransaction(beginNewTransaction = false, abortEmptyTransactionOnComplete = true)
    super.onCompletionSuccess()
  }

  override def onCompletionFailure(ex: Throwable): Unit = {
    log.debug("Aborting transaction due to stage failure")
    abortTransaction()
    batchOffsets.committingFailed()
    super.onCompletionFailure(ex)
  }

  private def commitTransaction(batch: NonemptyTransactionBatch, beginNewTransaction: Boolean): Unit = {
    val group = batch.group
    log.debug("Committing transaction for consumer group '{}' with offsets: {}", group, batch.offsets)
    val offsetMap = batch.offsetMap()
    producer.sendOffsetsToTransaction(offsetMap.asJava, group)
    producer.commitTransaction()
    log.debug("Committed transaction for consumer group '{}' with offsets: {}", group, batch.offsets)
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

  val onInternalCommitAckCb: AsyncCallback[Unit] = {
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

  private def abortTransaction(): Unit = {
    log.debug("Aborting transaction")
    producer.abortTransaction()
  }
}
