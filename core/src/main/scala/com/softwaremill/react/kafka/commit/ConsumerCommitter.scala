package com.softwaremill.react.kafka.commit

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Cancellable}
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.{Flush, TheEnd}
import kafka.common.TopicAndPartition
import kafka.consumer.KafkaConsumer
import kafka.message.MessageAndMetadata

import scala.util.Success

private[commit] class ConsumerCommitter[T](committerFactory: CommitterProvider, kafkaConsumer: KafkaConsumer[T])
    extends Actor with ActorLogging {

  val commitInterval = kafkaConsumer.commitInterval
  var scheduledFlush: Option[Cancellable] = None
  var partitionOffsetMap = OffsetMap()
  var committedOffsetMap = OffsetMap()
  val topic = kafkaConsumer.props.topic
  implicit val ec = context.dispatcher

  lazy val committerOpt: Option[OffsetCommitter] = createOffsetCommitter()

  override def preStart(): Unit = {
    super.preStart()
    committerOpt.foreach(_.start())
  }

  def scheduleFlush(): Unit = {
    if (scheduledFlush.isEmpty) {
      scheduledFlush = Some(context.system.scheduler.scheduleOnce(commitInterval, self, Flush))
    }
  }

  override def postStop(): Unit = {
    log.debug("Stopping consumer committer")
    scheduledFlush.foreach(_.cancel())
    committerOpt.foreach(_.stop())
    super.postStop()
  }

  def receive = {
    case TheEnd =>
      log.debug("Closing ConsumerCommitter")
      context.stop(self)
    case Failure =>
      log.error("Closing offset committer due to a failure")
      context.stop(self)
    case msg: MessageAndMetadata[_, T] => registerCommit(msg)
    case Flush => commitGatheredOffsets()
  }

  def registerCommit(msg: MessageAndMetadata[_, T]): Unit = {
    log.debug(s"Received commit request for partition ${msg.partition} and offset ${msg.offset}")
    val topicPartition = TopicAndPartition(topic, msg.partition)
    val last = partitionOffsetMap.lastOffset(topicPartition)
    updateOffsetIfLarger(msg, last)
  }

  def updateOffsetIfLarger(msg: MessageAndMetadata[_, T], last: Long): Unit = {
    if (msg.offset > last) {
      log.debug(s"Registering commit for partition ${msg.partition} and offset ${msg.offset}, last registered = $last")
      partitionOffsetMap = partitionOffsetMap.plusOffset(TopicAndPartition(topic, msg.partition), msg.offset)
      scheduleFlush()
    }
    else
      log.debug(s"Skipping commit for partition ${msg.partition} and offset ${msg.offset}, last registered is $last")
  }

  def commitGatheredOffsets(): Unit = {
    log.debug("Flushing offsets to commit")
    committerOpt.foreach { committer =>
      val offsetMapToFlush = partitionOffsetMap.diff(committedOffsetMap)
      if (offsetMapToFlush.nonEmpty)
        performFlush(committer, offsetMapToFlush)
    }
    scheduledFlush = None
  }

  def performFlush(committer: OffsetCommitter, offsetMapToFlush: OffsetMap): Unit = {
    val committedOffsetMapTry = committer.commit(offsetMapToFlush)
    committedOffsetMapTry match {
      case Success(resultOffsetMap) =>
        log.debug(s"committed offsets: $resultOffsetMap")
        committedOffsetMap = resultOffsetMap
      case scala.util.Failure(ex) =>
        log.error(ex, "Failed to commit offsets")
        committer.tryRestart() match {
          case scala.util.Failure(cause) =>
            log.error(cause, "Fatal error, cannot re-establish connection. Stopping native committer.")
            context.stop(self)
          case Success(()) =>
        }
    }
  }

  def createOffsetCommitter() = {
    val factoryOrError = committerFactory.create(kafkaConsumer)
    factoryOrError.failed.foreach(err => log.error(err.toString))
    factoryOrError.toOption
  }
}

object ConsumerCommitter {
  object Contract {

    object TheEnd

    object Flush

  }
}