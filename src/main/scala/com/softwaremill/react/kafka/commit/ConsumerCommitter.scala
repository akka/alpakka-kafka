package com.softwaremill.react.kafka.commit

import akka.actor.{ActorLogging, Actor}
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.{Flush, TheEnd}
import kafka.consumer.KafkaConsumer
import kafka.message.MessageAndMetadata

import scala.collection.mutable

private[commit] class ConsumerCommitter[T](kafkaConsumer: KafkaConsumer[T]) extends Actor with ActorLogging {

  var partitionOffsetMap: mutable.Map[Int, Long] = mutable.Map.empty

  override def preStart(): Unit = {
    super.preStart()
  }

  def commitGatheredOffsets(): Unit = {
    // TODO -> the actual magic that manually commits offsets
  }

  def receive = {
    case TheEnd => kafkaConsumer.close()
    case msg: MessageAndMetadata[_, T] =>
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