package com.softwaremill.react.kafka2

import java.util

import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object Consumer {
  def commitFromRecord = Flow[ConsumerRecord[_, _]].map { msg =>
    Map(new TopicPartition(msg.topic(), msg.partition()) -> new OffsetAndMetadata(msg.offset()))
  }
}

object ManualCommitConsumer {
  type CommitMsg = Map[TopicPartition, OffsetAndMetadata]
  type CommitConfirmation = Future[CommitMsg]

  case class ConsumerShape[K, V](
    commit: Inlet[CommitMsg],
    messages: Outlet[ConsumerRecord[K, V]],
    confirmation: Outlet[CommitConfirmation]
  ) extends Shape
  {
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
  class ManualCommitConsumer[K, V](consumerProvider: ConsumerProvider[K, V])
    extends GraphStageWithMaterializedValue[ManualCommitConsumer.ConsumerShape[K, V], ManualCommitConsumer.Control]
    with LazyLogging
  {
    import ManualCommitConsumer._
    val commitIn = Inlet[CommitMsg]("commitIn")
    val commitOut = Outlet[CommitConfirmation]("commitOut")
    val msgOut = Outlet[ConsumerRecord[K, V]]("msgOut")
    val shape = new Shape(commitIn, msgOut, commitOut)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
      import scala.concurrent.duration._
      val consumer = consumerProvider()
      var stopping = false

      val logic = new TimerGraphStageLogic(shape) {
        case object Poll

        var awaitingConfirmation = 0L
        var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

        def poll() = {
          def setupConsumer() = {
            if (isAvailable(msgOut)) {
              consumer.resume(consumer.assignment().toSeq: _*)
            } else {
              consumer.pause(consumer.assignment().toSeq: _*)
            }
          }
          def handleResult(records: ConsumerRecords[K, V]) = {
            if (!records.isEmpty) {
              logger.trace(s"Got messages - {}", records)
              require(!buffer.hasNext)
              require(isAvailable(msgOut))
              buffer = records.iterator()
              pushMsg(buffer.next())
            }
          }
          def needPolling = {
            (isAvailable(msgOut) && !buffer.hasNext) ||
              awaitingConfirmation > 0
          }

          if (stopping && !isClosed(msgOut)) {
            logger.debug("Stop producing messages")
            complete(msgOut)
          }
          if (needPolling) {
            setupConsumer()
            val msgs = consumer.poll(100)
            handleResult(msgs)
            if (needPolling) scheduleOnce(Poll, 100 millis)
          }
          if (stopping && isClosed(commitIn) && awaitingConfirmation == 0) {
            completeStage()
          }
        }

        def pushMsg(msg: ConsumerRecord[K, V]) = {
          logger.trace("Push element {}", msg)
          push(msgOut, msg)
        }

        setHandler(msgOut, new OutHandler {
          override def onPull(): Unit = {
            if (!buffer.hasNext) poll()
            else pushMsg(buffer.next())
          }
        })

        setHandler(commitOut, new OutHandler {
          override def onPull(): Unit = {
            tryPull(commitIn)
          }
        })

        setHandler(commitIn, new InHandler {
          override def onPush(): Unit = {
            val toCommit = grab(commitIn)
            val result = Promise[CommitMsg]()
            awaitingConfirmation += 1
            logger.trace(s"Start commit {}. Commits in progress {}", toCommit, awaitingConfirmation.toString)
            consumer.commitAsync(toCommit, new OffsetCommitCallback {
              override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
                awaitingConfirmation -= 1
                logger.trace(s"Commit completed {}. Commits in progress {}", toCommit, awaitingConfirmation.toString)
                val completion = Option(exception).map(Failure(_)).getOrElse(Success(toCommit))
                result.complete(completion)
              }
            })
            push(commitOut, result.future)
            poll()
          }

          override def onUpstreamFinish(): Unit = {
            poll()
          }
        })


        override protected def onTimer(timerKey: Any): Unit = {
          timerKey match {
            case Poll =>
              logger.trace("Scheduled poll")
              poll()
            case msg => super.onTimer(msg)
          }
        }

        override def postStop(): Unit = {
          logger.debug("Stage completed. Closing consumer")
          consumer.close()
          super.postStop()
        }
      }
      val control = new Control {
        override def stop(): Unit = {
          stopping = true
        }
      }
      (logic, control)
    }
  }
