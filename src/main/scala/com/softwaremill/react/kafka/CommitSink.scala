package com.softwaremill.react.kafka

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.scaladsl.Sink
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import kafka.message.MessageAndMetadata

object CommitSink {

  def create[T](
    customDispatcherName: Option[String] = None
  )(implicit actorSystem: ActorSystem) = {
    val actor = actorSystem.actorOf(Props(new ConsumerCommitterActor[T]()), "offsetCommitter")
    Sink.actorRef[KafkaMessage[T]](actor, "TheEnd")
  }
}

class ConsumerCommitterActor[T]() extends Actor with ActorLogging {

  def receive = {
    case "TheEnd" => log.info(">>>>>>>>>>>>>>>>>>>>> the end")
    case msg: MessageAndMetadata[_, T] => log.info(s">>>>>>>>>>>>>>>>>>>> received msg with offset ${msg.offset} and partition ${msg.partition}")
  }
}
