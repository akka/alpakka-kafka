/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util
import java.util.Collections
import java.util.concurrent.TimeoutException
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.control.NonFatal
import akka.Done
import akka.NotUsed
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.ClientTopicPartition
import akka.kafka.scaladsl.Consumer.CommittableOffsetBatch
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.TimeUnit
import scala.util.Try

/**
 * INTERNAL API
 */
private[kafka] object ConsumerStage {

  final case class CommittableOffsetImpl(override val partitionOffset: Consumer.PartitionOffset)(val stage: Committer)
      extends Consumer.CommittableOffset {
    override def commit(): Future[Done] =
      stage.commit(partitionOffset)
  }

  trait Committer {
    def commit(offset: Consumer.PartitionOffset): Future[Done]
    def commit(batch: CommittableOffsetBatchImpl): Future[Done]
  }

  final class CommittableOffsetBatchImpl(val offsets: Map[ClientTopicPartition, Long], val stages: Map[String, Committer])
      extends CommittableOffsetBatch {

    override def updated(committableOffset: Consumer.CommittableOffset): CommittableOffsetBatch = {
      val partitionOffset = committableOffset.partitionOffset
      val key = partitionOffset.key

      val newOffsets = offsets.updated(key, committableOffset.partitionOffset.offset)

      val stage = committableOffset match {
        case c: CommittableOffsetImpl => c.stage
        case _ => throw new IllegalArgumentException(
          s"Unknow CommittableOffset, got [${committableOffset.getClass.getName}], " +
            s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
      }

      val newStages = stages.get(key.clientId) match {
        case Some(s) =>
          require(s == stage, s"CommittableOffset [$committableOffset] origin stage must be same as other " +
            s"stage with same clientId. Expected [$s], got [$stage]")
          stages
        case None =>
          stages.updated(key.clientId, stage)
      }

      new CommittableOffsetBatchImpl(newOffsets, newStages)
    }

    override def getOffset(key: ClientTopicPartition): Option[Long] =
      offsets.get(key)

    override def toString(): String =
      s"CommittableOffsetBatch(${offsets.mkString("->")})"

    override def commit(): Future[Done] = {
      if (offsets.isEmpty)
        Future.successful(Done)
      else {
        stages.head._2.commit(this)
      }
    }
  }

}

/**
 * INTERNAL API
 */
private[kafka] class CommittableConsumerStage[K, V](settings: ConsumerSettings[K, V], consumerProvider: () => KafkaConsumer[K, V])
    extends GraphStageWithMaterializedValue[SourceShape[Consumer.CommittableMessage[K, V]], Consumer.Control] {
  import ConsumerStage._

  val out = Outlet[Consumer.CommittableMessage[K, V]]("messages")
  override val shape = SourceShape(out)

  require(
    settings.properties.contains(ConsumerConfig.CLIENT_ID_CONFIG),
    "client id must be defined when using committable offsets"
  )
  private val clientId = settings.properties(ConsumerConfig.CLIENT_ID_CONFIG)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val logic = new ConsumerStageLogic[K, V, Consumer.CommittableMessage[K, V]](
      settings,
      consumerProvider(), out, shape
    ) with ConsumerStage.Committer {

      private case object StopTimeout

      private val pollCommitTimeout = Some(settings.pollCommitTimeout)

      private var awaitingConfirmation = 0L
      private var stopping = false

      private val decrementConfirmation = getAsyncCallback[NotUsed] { _ =>
        awaitingConfirmation -= 1
        log.debug("Commits in progress {}", awaitingConfirmation.toString)
      }

      private val commitBatchCallback = getAsyncCallback[(CommittableOffsetBatchImpl, Promise[Done])] {
        case (batch, done) => commitBatchInternal(batch, done)
      }
      private val commitSingleCallback = getAsyncCallback[(Consumer.PartitionOffset, Promise[Done])] {
        case (offset, done) => commitSingleInternal(offset, done)
      }

      override protected def logSource: Class[_] = classOf[CommittableConsumerStage[K, V]]

      override def preStart(): Unit = {
        setKeepGoing(true)
      }

      override def pollTimeout(): Option[FiniteDuration] = {
        val data = super.pollTimeout()
        def commit = if (awaitingConfirmation > 0) pollCommitTimeout else None
        data.orElse(commit)
      }

      override protected def poll(): Unit = {
        super.poll()

        if (stopping && !isClosed(out)) {
          log.debug("Stop producing messages, commits in progress {}", awaitingConfirmation)
          complete(out)
        }

        if (stopping && awaitingConfirmation == 0) {
          completeStage()
        }
      }

      override protected def pushMsg(record: ConsumerRecord[K, V]): Unit = {
        val offset = Consumer.PartitionOffset(
          ClientTopicPartition(
            clientId = clientId,
            topic = record.topic,
            partition = record.partition
          ),
          offset = record.offset
        )
        val committable = CommittableOffsetImpl(offset)(this)
        val msg = Consumer.CommittableMessage(record.key, record.value, committable)
        log.debug("Push element {}", msg)
        push(out, msg)
      }

      // impl of ConsumerStage.Committer that is called from the outside via CommittableOffsetImpl
      override def commit(offset: Consumer.PartitionOffset): Future[Done] = {
        if (stoppedPromise.isCompleted)
          Future.failed(new IllegalStateException("ConsumerStage is stopped, offset commit not performed"))
        else {
          val done = Promise[Done]()
          scheduleCommitTimeout(done)
          commitSingleCallback.invoke((offset, done))
          done.future
        }
      }

      // impl of ConsumerStage.Committer that is called from the outside via CommittableOffsetBatchImpl
      override def commit(batch: CommittableOffsetBatchImpl): Future[Done] = {
        if (stoppedPromise.isCompleted)
          Future.failed(new IllegalStateException("ConsumerStage is stopped, offset commit not performed"))
        else {
          val done = Promise[Done]()
          scheduleCommitTimeout(done)
          commitBatchCallback.invoke((batch, done))
          done.future
        }
      }

      private def scheduleCommitTimeout(done: Promise[Done]): Unit = {
        // It's not safe to call `materializer` from the outside like this,
        // but in practice it will work after the stage has been started,
        // which is always the case for these commits. It's important to
        // not leave futures uncompleted.
        try {
          val c = materializer.scheduleOnce(settings.commitTimeout, new Runnable {
            override def run(): Unit = {
              done.tryFailure(new TimeoutException(
                s"Offset commit timed out after ${settings.commitTimeout.toMillis} ms"
              ))
            }
          })
          done.future.onComplete(__ => c.cancel())(materializer.executionContext)
        }
        catch {
          case NonFatal(e) =>
            done.tryFailure(new IllegalStateException("ConsumerStage is not started, offset commit not performed"))
        }
      }

      private def commitSingleInternal(partitionOffset: Consumer.PartitionOffset, done: Promise[Done]): Unit = {
        // committed offset should be the next message the application will consume, i.e. +1
        val offsets = Collections.singletonMap(
          new TopicPartition(partitionOffset.key.topic, partitionOffset.key.partition),
          new OffsetAndMetadata(partitionOffset.offset + 1)
        )
        commitAsync(offsets, done)
      }

      private def commitBatchInternal(batch: CommittableOffsetBatchImpl, done: Promise[Done]): Unit = {
        val (mine, others) = batch.offsets.partition { case (key, value) => key.clientId == clientId }
        // committed offset should be the next message the application will consume, i.e. +1
        val offsets = mine.map { case (key, value) => new TopicPartition(key.topic, key.partition) -> new OffsetAndMetadata(value + 1) }
        if (others.isEmpty)
          commitAsync(offsets.asJava, done)
        else {
          val done2 = Promise[Done]()
          commitAsync(offsets.asJava, done2)
          val remainingBatch = new CommittableOffsetBatchImpl(others, batch.stages - clientId)
          val done3 = remainingBatch.stages.head._2.commit(remainingBatch)
          implicit val ec = materializer.executionContext
          done.completeWith(done2.future.flatMap(_ => done3))
        }
      }

      private def commitAsync(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], done: Promise[Done]): Unit = {
        awaitingConfirmation += 1
        log.debug("Start commit {}. Commits in progress {}", offsets, awaitingConfirmation)
        consumer.commitAsync(offsets, new OffsetCommitCallback {
          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
            if (exception == null)
              done.success(Done)
            else
              done.failure(exception)
            if (log.isDebugEnabled)
              log.debug("Commit completed {} - {}", offsets, done.future.value)
            decrementConfirmation.invoke(NotUsed)
          }
        })
        // TODO This can be optimized to scheduling poll after
        //      https://issues.apache.org/jira/browse/KAFKA-3412 has been fixed
        poll()
      }

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case StopTimeout =>
            log.debug("Stop timeout, commits in progress {}", awaitingConfirmation)
            completeStage()
          case msg => super.onTimer(msg)
        }
      }

      override protected def stopInternal(): Unit = {
        if (!stopping) {
          stopping = true
          scheduleOnce(StopTimeout, settings.stopTimeout)
          poll()
        }
      }

    }
    (logic, logic)
  }
}

/**
 * INTERNAL API
 */
private[kafka] class PlainConsumerStage[K, V](settings: ConsumerSettings[K, V], consumerProvider: () => KafkaConsumer[K, V])
    extends GraphStageWithMaterializedValue[SourceShape[ConsumerRecord[K, V]], Consumer.Control] {
  import ConsumerStage._

  val out = Outlet[ConsumerRecord[K, V]]("messages")
  override val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val logic = new ConsumerStageLogic[K, V, ConsumerRecord[K, V]](settings, consumerProvider(), out, shape) {

      override protected def logSource: Class[_] = classOf[PlainConsumerStage[K, V]]

      override protected def pushMsg(record: ConsumerRecord[K, V]): Unit = {
        log.debug("Push element {}", record)
        push(out, record)
      }

      override protected def stopInternal(): Unit =
        completeStage()
    }
    (logic, logic)
  }
}

/**
 * INTERNAL API
 */
private[kafka] abstract class ConsumerStageLogic[K, V, Out](
  settings: ConsumerSettings[K, V],
  val consumer: KafkaConsumer[K, V],
  out: Outlet[Out],
  shape: SourceShape[Out]
) extends TimerGraphStageLogic(shape)
    with Consumer.Control with StageLogging {
  import ConsumerStage._

  val stoppedPromise = Promise[Done]

  private case object Poll
  private val pollDataTimeout = Some(settings.pollTimeout)
  private var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
  private var pollScheduled = false

  private val pollCallback = getAsyncCallback[Unit] { _ => schedulePoll() }
  private val stopCallback = getAsyncCallback[Unit] { _ => stopInternal() }

  setHandler(out, new OutHandler {
    override def onPull(): Unit = {
      // TODO preemptively try to fetch new batches when we are below a low watermark?
      //      We also should pay attention to how it optimized in kafka client.
      //      There is also prefetch algorithm in it.
      if (!buffer.hasNext) poll()
      else pushMsg(buffer.next())
    }

    override def onDownstreamFinish(): Unit = {
      stopInternal()
    }
  })

  protected def pushMsg(record: ConsumerRecord[K, V]): Unit

  protected def stopInternal(): Unit

  private def schedulePoll() = {
    if (!pollScheduled) {
      pollScheduled = true
      scheduleOnce(Poll, settings.pollInterval)
    }
  }

  protected def pollTimeout(): Option[FiniteDuration] =
    if (isAvailable(out) && buffer.isEmpty) pollDataTimeout
    else None

  protected def poll(): Unit = {
    try {
      def toggleConsumption() = {
        // note that consumer.assignment() is automatically changed when using
        // dynamic subscriptions and we must use the current value to resume/pause
        if (isAvailable(out))
          consumer.resume(consumer.assignment().asScala.toArray: _*)
        else
          consumer.pause(consumer.assignment().asScala.toArray: _*)
      }

      def handleResult(records: ConsumerRecords[K, V]) = {
        if (!records.isEmpty) {
          if (log.isDebugEnabled)
            log.debug("Got {} messages, out isAvailable {}", records.count, isAvailable(out))
          require(!buffer.hasNext)
          buffer = records.iterator().asScala
          if (isAvailable(out))
            pushMsg(buffer.next())
        }
      }

      pollTimeout() match {
        case Some(timeout) =>
          toggleConsumption()
          // TODO poll is blocking, perhaps we can use some adaptive algorithm
          val msgs = consumer.poll(timeout.toMillis)
          handleResult(msgs)
          if (pollTimeout.isDefined) schedulePoll()
        case None =>
      }
    }
    catch {
      case NonFatal(e) =>
        log.error(e, "Error in poll")
        throw e
    }

  }

  override protected def onTimer(timerKey: Any): Unit = {
    timerKey match {
      case Poll =>
        log.debug("Scheduled poll")
        pollScheduled = false
        poll()
    }
  }

  override def postStop(): Unit = {
    log.debug("Stage completed. Closing kafka consumer")
    val stopTimeoutTask = materializer match {
      case a: ActorMaterializer =>
        Some(a.system.scheduler.scheduleOnce(settings.closeTimeout) {
          consumer.wakeup()
        }(materializer.executionContext))
      case _ =>
        // not much we can do without scheduler
        None
    }
    // close() is blocking (stupid api that doesn't take timeout parameters)
    // WakeupException will be thrown by close() if the wakeup from stopTimeout is triggered
    Try(consumer.close())
    stopTimeoutTask.foreach(_.cancel())
    stoppedPromise.success(Done)
    super.postStop()
  }

  // impl of Consumer.Control that is called from outside via materialized value
  override def stop(): Future[Done] = {
    log.debug("Stopping consumer stage")
    stopCallback.invoke(())
    stopped
  }

  // impl of Consumer.Control that is called from outside via materialized value
  override def stopped: Future[Done] =
    stoppedPromise.future
}

