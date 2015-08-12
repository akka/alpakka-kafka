package com.softwaremill.react.kafka

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.scaladsl.Sink

object CommitSink {

  def create(
    customDispatcherName: Option[String] = None
  )(implicit actorSystem: ActorSystem) = {
    val actor = actorSystem.actorOf(Props(new ConsumerCommitterActor()), "offsetCommitter")
    Sink.actorRef[Long](actor, "TheEnd")
  }
}

class ConsumerCommitterActor() extends Actor with ActorLogging {

  def receive = {
    case "TheEnd" => log.info(">>>>>>>>>>>>>>>>>>>>> the end")
    case num: Long => log.info(s">>>>>>>>>>>>>>>>>>>> received number $num")
  }
}
