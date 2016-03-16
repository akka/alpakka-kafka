package com.softwaremill.react.kafka.commit

import akka.actor.Status.Failure
import akka.actor._
import com.softwaremill.react.kafka.ConsumerProperties
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.{Flush, TheEnd}
import com.softwaremill.react.kafka.commit.ConsumerCommitter.{CommitAck, CommitOffsets}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * This should become deprecated as soon as we remove KafkaActorSubscriber/KafkaActorPublisher and related stuff.
 */
private[commit] class ConsumerCommitter[K, V](
    consumerActor: ActorRef,
    consumerProperties: ConsumerProperties[_, _]
) extends Actor with ActorLogging {

  val DefaultCommitInterval = 30 seconds
  val commitInterval = consumerProperties.commitInterval.getOrElse(DefaultCommitInterval)
  var scheduledFlush: Option[Cancellable] = None
  var partitionOffsetMap = OffsetMap()
  var committedOffsetMap = OffsetMap()
  val topic = consumerProperties.topic
  implicit val ec = context.dispatcher

  def scheduleFlush(): Unit = {
    if (scheduledFlush.isEmpty) {
      scheduledFlush = Some(context.system.scheduler.scheduleOnce(commitInterval, self, Flush))
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    context.watch(consumerActor)
    ()
  }

  override def postStop(): Unit = {
    scheduledFlush.foreach(_.cancel())
    super.postStop()
  }

  def receive = {
    case TheEnd =>
      log.debug("Closing ConsumerCommitter")
      context.stop(self)
    case Failure =>
      log.error("Closing offset committer due to a failure")
      context.stop(self)
    case msg: ConsumerRecord[_, V] => registerCommit(msg)
    case CommitAck(offsetMap) => handleAck(offsetMap)
    case Terminated(_) =>
      log.warning("Terminating the consumer committer due to the death of the consumer actor.")
      context.stop(self)
    case Flush => commitGatheredOffsets()
  }

  def registerCommit(msg: ConsumerRecord[_, _]): Unit = {
    log.debug(s"Received commit request for partition ${msg.partition} and offset ${msg.offset}")
    val topicPartition = new TopicPartition(topic, msg.partition)
    val last = partitionOffsetMap.lastOffset(topicPartition)
    updateOffsetIfLarger(msg, last)
  }

  def updateOffsetIfLarger(msg: ConsumerRecord[_, _], last: Long): Unit = {
    if (msg.offset > last) {
      log.debug(s"Registering commit for partition ${msg.partition} and offset ${msg.offset}, last registered = $last")
      partitionOffsetMap = partitionOffsetMap.plusOffset(new TopicPartition(topic, msg.partition), msg.offset)
      scheduleFlush()
    }
    else
      log.debug(s"Skipping commit for partition ${msg.partition} and offset ${msg.offset}, last registered is $last")
  }

  def commitGatheredOffsets(): Unit = {
    log.debug("Flushing offsets to commit")
    scheduledFlush = None
    val offsetMapToFlush = partitionOffsetMap.diff(committedOffsetMap)
    if (offsetMapToFlush.nonEmpty)
      consumerActor ! CommitOffsets(offsetMapToFlush)
  }

  private def handleAck(offsetMap: OffsetMap): Unit = {
    committedOffsetMap = OffsetMap(offsetMap.map.mapValues(_ - 1))
  }

}

object ConsumerCommitter {
  case class CommitOffsets(offsets: OffsetMap)
  case class CommitAck(offsets: OffsetMap)
  object Contract {
    object TheEnd
    object Flush
  }
}
