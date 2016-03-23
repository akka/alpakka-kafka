/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka.internal

import java.util

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source}
import akka.stream.stage._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.language.reflectiveCalls
import scala.util.{Failure, Success}

object Consumer {
  def record2commit: Flow[ConsumerRecord[_, _], Map[TopicPartition, OffsetAndMetadata], NotUsed] =
    Flow[ConsumerRecord[_, _]]
      .map { msg =>
        Map(new TopicPartition(msg.topic(), msg.partition()) -> new OffsetAndMetadata(msg.offset()))
      }

  def batchCommit(maxCount: Int, maxDuration: FiniteDuration): Flow[Map[TopicPartition, OffsetAndMetadata], Map[TopicPartition, OffsetAndMetadata], NotUsed] =
    Flow[Map[TopicPartition, OffsetAndMetadata]]
      .groupedWithin(maxCount, maxDuration)
      .map {
        _.flatten.groupBy(_._1).map {
          case (topic, offsets) => (topic, offsets.map(_._2).maxBy(_.offset()))
        }.toMap
      }

  def apply[K, V](provider: ConsumerProvider[K, V]) = new ManualCommitConsumer(provider)

  def source[K, V](consumerProvider: ConsumerProvider[K, V]): Source[ConsumerRecord[K, V], ManualCommitConsumer.Control] = {
    Source.fromGraph(
      GraphDSL.create(new ManualCommitConsumer[K, V](consumerProvider)) { implicit b => consumer =>
        import GraphDSL.Implicits._
        consumer.commit <~ Source.empty
        consumer.confirmation ~> Sink.ignore
        SourceShape(consumer.messages)
      }
    )
  }

  def flow[K, V](consumerProvider: ConsumerProvider[K, V]): Flow[Map[TopicPartition, OffsetAndMetadata], ConsumerRecord[K, V], ManualCommitConsumer.Control] = {
    Flow.fromGraph(
      GraphDSL.create(new ManualCommitConsumer[K, V](consumerProvider)) { implicit b => consumer =>
        import GraphDSL.Implicits._
        consumer.confirmation ~> Sink.ignore
        FlowShape(consumer.commit, consumer.messages)
      }
    )
  }
}

object ManualCommitConsumer {
  type CommitMsg = Map[TopicPartition, OffsetAndMetadata]
  type CommitConfirmation = Future[CommitMsg]

  case class ConsumerShape[K, V](
      commit: Inlet[CommitMsg],
      messages: Outlet[ConsumerRecord[K, V]],
      confirmation: Outlet[CommitConfirmation]
  ) extends Shape {
    override def inlets: immutable.Seq[Inlet[_]] = immutable.Seq(commit)
    override def outlets: immutable.Seq[Outlet[_]] = immutable.Seq(messages, confirmation)
    override def deepCopy(): Shape = ConsumerShape(
      commit.carbonCopy(),
      messages.carbonCopy(),
      confirmation.carbonCopy()
    )
    override def copyFromPorts(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]): Shape = {
      require(inlets.size == 1)
      require(outlets.size == 2)
      ConsumerShape(
        inlets.head.as[CommitMsg],
        outlets(0).as[ConsumerRecord[K, V]],
        outlets(1).as[CommitConfirmation]
      )
    }
  }

  trait Control {
    def stop(): Unit
  }
}

class ManualCommitConsumer[K, V](consumerProvider: () => KafkaConsumer[K, V])
    extends GraphStageWithMaterializedValue[ManualCommitConsumer.ConsumerShape[K, V], ManualCommitConsumer.Control]
    with LazyLogging {
  import ManualCommitConsumer._
  val commitIn = Inlet[CommitMsg]("commitIn")
  val confirmationOut = Outlet[CommitConfirmation]("confirmationOut")
  val messagesOut = Outlet[ConsumerRecord[K, V]]("messagesOut")
  val shape = new ConsumerShape(commitIn, messagesOut, confirmationOut)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    import scala.concurrent.duration._
    val consumer = consumerProvider()
    var stopping = false

    val logic = new TimerGraphStageLogic(shape) {
      private case object Poll
      private val POLL_INTERVAL = 100.millis
      private val POLL_DATA_TIMEOUT = 50.millis
      private val POLL_COMMIT_TIMEOUT = 1.millis
      private var awaitingConfirmation = 0L
      private var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
      private var pollScheduled = false

      private def schedulePoll() = {
        if (!pollScheduled) {
          pollScheduled = true
          scheduleOnce(Poll, POLL_INTERVAL)
        }
      }

      private def poll() = {
        def setupConsumer() = {
          if (isAvailable(messagesOut)) {
            consumer.resume(consumer.assignment().toSeq: _*)
          }
          else {
            consumer.pause(consumer.assignment().toSeq: _*)
          }
        }
        def handleResult(records: ConsumerRecords[K, V]) = {
          if (!records.isEmpty) {
            logger.trace(s"Got messages - {}", records)
            require(!buffer.hasNext)
            require(isAvailable(messagesOut))
            buffer = records.iterator()
            pushMsg(buffer.next())
          }
        }
        def pollTimeout = {
          val data = Option(POLL_DATA_TIMEOUT).filter(_ => isAvailable(messagesOut) && !buffer.hasNext)
          val commit = Option(POLL_COMMIT_TIMEOUT).filter(_ => awaitingConfirmation > 0)
          data.orElse(commit)
        }

        pollTimeout match {
          case Some(timeout) =>
            setupConsumer()
            val msgs = consumer.poll(timeout.toMillis)
            handleResult(msgs)
            if (pollTimeout.isDefined) schedulePoll()
          case None =>
        }

        if (stopping && !isClosed(messagesOut)) {
          logger.debug("Stop producing messages")
          complete(messagesOut)
        }

        if (stopping && isClosed(commitIn) && awaitingConfirmation == 0) {
          completeStage()
        }
      }

      val pollCallback = getAsyncCallback[Unit] { _ => schedulePoll() }

      private def pushMsg(msg: ConsumerRecord[K, V]) = {
        logger.trace("Push element {}", msg)
        push(messagesOut, msg)
      }

      setHandler(messagesOut, new OutHandler {
        override def onPull(): Unit = {
          if (!buffer.hasNext) poll()
          else pushMsg(buffer.next())
        }
      })

      setHandler(confirmationOut, new OutHandler {
        override def onPull(): Unit = {
          tryPull(commitIn)
        }

        override def onDownstreamFinish(): Unit = {
          if (!isClosed(commitIn)) {
            cancel(commitIn)
          }
          poll()
        }
      })

      private val decrementConfirmation = getAsyncCallback[Unit] { _ =>
        awaitingConfirmation -= 1
        logger.trace(s"Commits in progress {}", awaitingConfirmation.toString)
      }

      setHandler(commitIn, new InHandler {
        override def onPush(): Unit = {
          val toCommit = grab(commitIn)
          val result = Promise[CommitMsg]()
          awaitingConfirmation += 1
          logger.trace(s"Start commit {}. Commits in progress {}", toCommit, awaitingConfirmation.toString)
          consumer.commitAsync(toCommit, new OffsetCommitCallback {
            override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
              val completion = Option(exception).map(Failure(_)).getOrElse(Success(toCommit))
              logger.trace(s"Commit completed {} - {}", toCommit, completion)
              result.complete(completion)
              decrementConfirmation.invoke(())
            }
          })
          push(confirmationOut, result.future)
          poll()
        }

        override def onUpstreamFinish(): Unit = {
          logger.trace("Commit upstream finished")
          poll()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          logger.trace("Commit upstream failed {}", ex)
          super.onUpstreamFailure(ex)
        }
      })

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case Poll =>
            logger.trace("Scheduled poll")
            pollScheduled = false
            poll()
          case msg => super.onTimer(msg)
        }
      }

      override def postStop(): Unit = {
        logger.debug("Stage completed. Closing kafka consumer")
        consumer.close()
        super.postStop()
      }
    }

    val control = new Control {
      override def stop(): Unit = {
        logger.debug("Stopping consumer shape")
        stopping = true
        logic.pollCallback.invoke(())
      }
    }
    (logic, control)
  }
}
