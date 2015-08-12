package com.softwaremill.react.kafka.commit

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Cancellable}
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.{Flush, TheEnd}
import kafka.consumer.KafkaConsumer
import kafka.message.MessageAndMetadata

import scala.collection.mutable

private[commit] class ConsumerCommitter[T](
    kafkaConsumer: KafkaConsumer[T]
) extends Actor with ActorLogging {

  val commitInterval = kafkaConsumer.commitInterval
  var scheduledFlush: Option[Cancellable] = None
  var partitionOffsetMap: mutable.Map[Int, Long] = mutable.Map.empty

  override def preStart(): Unit = {
    super.preStart()
    scheduledFlush.foreach(_.cancel())
    scheduleFlush()
  }

  def scheduleFlush(): Unit = {
    implicit val ec = context.dispatcher
    scheduledFlush = Some(context.system.scheduler.scheduleOnce(commitInterval, self, Flush))
  }

  def commitGatheredOffsets(): Unit = {
    // TODO -> the actual magic that manually commits offsets
    log.debug("Flushing offsets to commit")
    scheduleFlush()
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
      if (msg.offset > partitionOffsetMap.getOrElse(msg.partition, 0L))
        partitionOffsetMap.update(msg.partition, msg.offset)
    case Flush => commitGatheredOffsets()
  }
}

object ConsumerCommitter {
  object Contract {

    object TheEnd

    object Flush

  }
}