package com.softwaremill.react.kafka.commit

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Cancellable}
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.{Flush, TheEnd}
import kafka.consumer.KafkaConsumer
import kafka.message.MessageAndMetadata

import scala.util.{Success, Try}

private[commit] class ConsumerCommitter[T](committerFactory: CommitterFactory, kafkaConsumer: KafkaConsumer[T])
    extends Actor with ActorLogging {

  val commitInterval = kafkaConsumer.commitInterval
  var scheduledFlush: Option[Cancellable] = None
  var partitionOffsetMap: OffsetMap = Map.empty
  val topic = kafkaConsumer.props.topic
  lazy val committerOpt: Option[OffsetCommitter] = createOffsetCommitter()

  override def preStart(): Unit = {
    super.preStart()
    scheduleFlush()
    committerOpt.foreach(_.start())
  }

  def scheduleFlush(): Unit = {
    implicit val ec = context.dispatcher
    scheduledFlush = Some(context.system.scheduler.scheduleOnce(commitInterval, self, Flush))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    scheduledFlush.foreach(_.cancel())
  }

  override def postRestart(reason: Throwable): Unit = {
    scheduleFlush()
  }

  override def postStop(): Unit = {
    super.postStop()
    committerOpt.foreach(_.stop())
  }

  def receive = {
    case TheEnd =>
      log.debug("Closing ConsumerCommitter")
      kafkaConsumer.close()
    case Failure =>
      log.error("Closing offset committer due to a failure")
      kafkaConsumer.close()
    case msg: MessageAndMetadata[_, T] =>
      log.debug(s"Received commit request for offset ${msg.offset} and partition ${msg.partition}")
      if (msg.offset > partitionOffsetMap.getOrElse((topic, msg.partition), 0L))
        partitionOffsetMap = partitionOffsetMap + ((topic, msg.partition) -> msg.offset)
    case Flush => commitGatheredOffsets()
  }

  def commitGatheredOffsets(): Unit = {
    log.debug("Flushing offsets to commit")
    committerOpt.foreach { committer =>
      val resultOffsetMap = Try(committer.commit(partitionOffsetMap))
      resultOffsetMap match {
        case Success(newOffsets) => clearCommittedOffsets(newOffsets)
        case scala.util.Failure(ex) => log.error(ex, "Failed to commit offsets")
      }
    }
    scheduleFlush()
  }

  def clearCommittedOffsets(resultOffsetMap: OffsetMap): Unit = {
    partitionOffsetMap = (partitionOffsetMap.toSet diff resultOffsetMap.toSet).toMap
  }

  def createOffsetCommitter() = {
    val factoryOrError = committerFactory.create(kafkaConsumer)
    factoryOrError.left.foreach(err => log.error(err.toString))
    factoryOrError.right.toOption
  }
}

object ConsumerCommitter {
  object Contract {

    object TheEnd

    object Flush

  }
}