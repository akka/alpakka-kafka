package com.softwaremill.react.kafka.commit

import akka.actor.{Props, ActorSystem}
import akka.stream.scaladsl.Sink
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.TheEnd
import kafka.consumer.KafkaConsumer

private[kafka] object CommitSink {

  def create[T](
    kafkaConsumer: KafkaConsumer[T],
    customDispatcherName: Option[String] = None
  )(implicit actorSystem: ActorSystem) = {
    val actor = actorSystem.actorOf(Props(new ConsumerCommitter(kafkaConsumer)), "offsetCommitter")
    Sink.actorRef[KafkaMessage[T]](actor, TheEnd)
  }
}
