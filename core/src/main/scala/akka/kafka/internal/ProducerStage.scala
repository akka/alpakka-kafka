/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffset}
import akka.kafka.ProducerMessage._
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream._
import akka.stream.stage._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Callback, Producer, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
private[kafka] object ProducerStage {

  class DefaultProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
      val closeTimeout: FiniteDuration,
      val closeProducerOnStop: Boolean,
      val producerProvider: () => Producer[K, V]
  ) extends GraphStage[FlowShape[IN, Future[OUT]]]
      with ProducerStage[K, V, P, IN, OUT] {

    override def createLogic(inheritedAttributes: Attributes) =
      new DefaultProducerStageLogic(this, producerProvider(), inheritedAttributes)
  }

  class TransactionProducerStage[K, V, P](
      val closeTimeout: FiniteDuration,
      val closeProducerOnStop: Boolean,
      val producerProvider: () => Producer[K, V],
      commitInterval: FiniteDuration
  ) extends GraphStage[FlowShape[Envelope[K, V, P], Future[Results[K, V, P]]]]
      with ProducerStage[K, V, P, Envelope[K, V, P], Results[K, V, P]] {

    override def createLogic(inheritedAttributes: Attributes) =
      new TransactionProducerStageLogic(this, producerProvider(), inheritedAttributes, commitInterval)
  }

  trait ProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]] {
    val closeTimeout: FiniteDuration
    val closeProducerOnStop: Boolean
    val producerProvider: () => Producer[K, V]

    val in: Inlet[IN] = Inlet[IN]("messages")
    val out: Outlet[Future[OUT]] = Outlet[Future[OUT]]("result")
    val shape: FlowShape[IN, Future[OUT]] = FlowShape(in, out)
  }

  /**
   * Default Producer State Logic
   */
  class DefaultProducerStageLogic[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
      stage: ProducerStage[K, V, P, IN, OUT],
      producer: Producer[K, V],
      inheritedAttributes: Attributes
  ) extends TimerGraphStageLogic(stage.shape)
      with StageLogging
      with MessageCallback[K, V, P]
      with ProducerCompletionState {

    lazy val decider: Decider =
      inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
    val awaitingConfirmation = new AtomicInteger(0)
    @volatile var inIsClosed = false
    var completionState: Option[Try[Unit]] = None

    override protected def logSource: Class[_] = classOf[DefaultProducerStage[_, _, _, _, _]]

    def checkForCompletion(): Unit =
      if (isClosed(stage.in) && awaitingConfirmation.get == 0) {
        completionState match {
          case Some(Success(_)) => onCompletionSuccess()
          case Some(Failure(ex)) => onCompletionFailure(ex)
          case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
        }
      }

    override def onCompletionSuccess(): Unit = completeStage()

    override def onCompletionFailure(ex: Throwable): Unit = failStage(ex)

    val checkForCompletionCB: AsyncCallback[Unit] = getAsyncCallback[Unit] { _ =>
      checkForCompletion()
    }

    val failStageCb: AsyncCallback[Throwable] = getAsyncCallback[Throwable] { ex =>
      failStage(ex)
    }

    override val onMessageAckCb: AsyncCallback[Envelope[K, V, P]] = getAsyncCallback[Envelope[K, V, P]] { _ =>
      }

    setHandler(stage.out, new OutHandler {
      override def onPull(): Unit = tryPull(stage.in)
    })

    setHandler(
      stage.in,
      new InHandler {
        override def onPush(): Unit = produce(grab(stage.in))

        override def onUpstreamFinish(): Unit = {
          inIsClosed = true
          completionState = Some(Success(()))
          checkForCompletion()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          inIsClosed = true
          completionState = Some(Failure(ex))
          checkForCompletion()
        }
      }
    )

    def produce(in: Envelope[K, V, P]): Unit =
      in match {
        case msg: Message[K, V, P] =>
          val r = Promise[Result[K, V, P]]
          awaitingConfirmation.incrementAndGet()
          producer.send(msg.record, sendCallback(r, onSuccess = metadata => {
            onMessageAckCb.invoke(msg)
            r.success(Result(metadata, msg))
          }))
          val future = r.future.asInstanceOf[Future[OUT]]
          push(stage.out, future)

        case multiMsg: MultiMessage[K, V, P] =>
          val promises = for {
            msg <- multiMsg.records
          } yield {
            val r = Promise[MultiResultPart[K, V]]
            awaitingConfirmation.incrementAndGet()
            producer.send(msg, sendCallback(r, onSuccess = metadata => r.success(MultiResultPart(metadata, msg))))
            r.future
          }
          implicit val ec: ExecutionContext = this.materializer.executionContext
          val res = Future.sequence(promises).map { parts =>
            onMessageAckCb.invoke(multiMsg)
            MultiResult(parts, multiMsg.passThrough)
          }
          val future = res.asInstanceOf[Future[OUT]]
          push(stage.out, future)

        case _: PassThroughMessage[K, V, P] =>
          onMessageAckCb.invoke(in)
          val future = Future.successful(PassThroughResult[K, V, P](in.passThrough)).asInstanceOf[Future[OUT]]
          push(stage.out, future)

      }

    private def sendCallback(promise: Promise[_], onSuccess: RecordMetadata => Unit): Callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) onSuccess(metadata)
        else
          decider(exception) match {
            case Supervision.Stop =>
              if (stage.closeProducerOnStop) {
                producer.close(0, TimeUnit.MILLISECONDS)
              }
              failStageCb.invoke(exception)
            case _ =>
              promise.failure(exception)
          }
        if (awaitingConfirmation.decrementAndGet() == 0 && inIsClosed)
          checkForCompletionCB.invoke(())
      }
    }

    override def postStop(): Unit = {
      log.debug("Stage completed")

      if (stage.closeProducerOnStop) {
        try {
          // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
          producer.flush()
          producer.close(stage.closeTimeout.toMillis, TimeUnit.MILLISECONDS)
          log.debug("Producer closed")
        } catch {
          case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
        }
      }

      super.postStop()
    }
  }

  /**
   * Transaction (Exactly-Once) Producer State Logic
   */
  class TransactionProducerStageLogic[K, V, P](stage: ProducerStage[K, V, P, Envelope[K, V, P], Results[K, V, P]],
                                               producer: Producer[K, V],
                                               inheritedAttributes: Attributes,
                                               commitInterval: FiniteDuration)
      extends DefaultProducerStageLogic[K, V, P, Envelope[K, V, P], Results[K, V, P]](stage,
                                                                                      producer,
                                                                                      inheritedAttributes)
      with StageLogging
      with MessageCallback[K, V, P]
      with ProducerCompletionState {
    private val commitSchedulerKey = "commit"
    private val messageDrainInterval = 10.milliseconds

    private var batchOffsets = TransactionBatch.empty

    override def preStart(): Unit = {
      initTransactions()
      beginTransaction()
      resumeDemand(tryToPull = false)
      scheduleOnce(commitSchedulerKey, commitInterval)
    }

    private def resumeDemand(tryToPull: Boolean = true): Unit = {
      setHandler(stage.out, new OutHandler {
        override def onPull(): Unit = tryPull(stage.in)
      })
      // kick off demand for more messages if we're resuming demand
      if (tryToPull && !hasBeenPulled(stage.in)) {
        tryPull(stage.in)
      }
    }

    private def suspendDemand(): Unit =
      setHandler(
        stage.out,
        new OutHandler {
          // suspend demand while a commit is in process so we can drain any outstanding message acknowledgements
          override def onPull(): Unit = ()
        }
      )

    override protected def onTimer(timerKey: Any): Unit =
      if (timerKey == commitSchedulerKey) {
        maybeCommitTransaction()
      }

    private def maybeCommitTransaction(beginNewTransaction: Boolean = true): Unit = {
      val awaitingConf = awaitingConfirmation.get
      batchOffsets match {
        case batch: NonemptyTransactionBatch if awaitingConf == 0 =>
          commitTransaction(batch, beginNewTransaction)
        case _ if awaitingConf > 0 =>
          suspendDemand()
          scheduleOnce(commitSchedulerKey, messageDrainInterval)
        case _ =>
          scheduleOnce(commitSchedulerKey, commitInterval)
      }
    }

    override val onMessageAckCb: AsyncCallback[Envelope[K, V, P]] =
      getAsyncCallback[Envelope[K, V, P]](_.passThrough match {
        case o: ConsumerMessage.PartitionOffset => batchOffsets = batchOffsets.updated(o)
        case _ =>
      })

    override def onCompletionSuccess(): Unit = {
      log.debug("Committing final transaction before shutdown")
      cancelTimer(commitSchedulerKey)
      maybeCommitTransaction(beginNewTransaction = false)
      super.onCompletionSuccess()
    }

    override def onCompletionFailure(ex: Throwable): Unit = {
      log.debug("Aborting transaction due to stage failure")
      abortTransaction()
      super.onCompletionFailure(ex)
    }

    private def commitTransaction(batch: NonemptyTransactionBatch, beginNewTransaction: Boolean): Unit = {
      val group = batch.group
      log.debug("Committing transaction for consumer group '{}' with offsets: {}", group, batch.offsetMap())
      val offsetMap = batch.offsetMap().asJava
      producer.sendOffsetsToTransaction(offsetMap, group)
      producer.commitTransaction()
      batchOffsets = TransactionBatch.empty
      if (beginNewTransaction) {
        beginTransaction()
        resumeDemand()
        scheduleOnce(commitSchedulerKey, commitInterval)
      }
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

  sealed trait ProducerCompletionState {
    def onCompletionSuccess(): Unit
    def onCompletionFailure(ex: Throwable): Unit
  }

  sealed trait MessageCallback[K, V, P] {
    def awaitingConfirmation: AtomicInteger
    def onMessageAckCb: AsyncCallback[Envelope[K, V, P]]
  }

  object TransactionBatch {
    def empty: TransactionBatch = new EmptyTransactionBatch()
  }

  sealed trait TransactionBatch {
    def updated(partitionOffset: PartitionOffset): TransactionBatch
  }

  final class EmptyTransactionBatch extends TransactionBatch {
    override def updated(partitionOffset: PartitionOffset): TransactionBatch =
      new NonemptyTransactionBatch(partitionOffset)
  }

  final class NonemptyTransactionBatch(head: PartitionOffset,
                                       tail: Map[GroupTopicPartition, Long] = Map[GroupTopicPartition, Long]())
      extends TransactionBatch {
    private val offsets = tail + (head.key -> head.offset)

    def group: String = head.key.groupId
    def offsetMap(): Map[TopicPartition, OffsetAndMetadata] = offsets.map {
      case (gtp, offset) => new TopicPartition(gtp.topic, gtp.partition) -> new OffsetAndMetadata(offset + 1)
    }

    override def updated(partitionOffset: PartitionOffset): TransactionBatch = {
      require(
        group == partitionOffset.key.groupId,
        s"Transaction batch must contain messages from exactly 1 consumer group. $group != ${partitionOffset.key.groupId}"
      )
      new NonemptyTransactionBatch(partitionOffset, offsets)
    }
  }
}
