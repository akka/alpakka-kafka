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
    val initialProps = Props(new ConsumerCommitter(new CommitterFactory(), kafkaConsumer))
    val props = customDispatcherName.map(initialProps.withDispatcher).getOrElse(initialProps)
    val actor = actorSystem.actorOf(props, "offsetCommitter")
    Sink.actorRef[KafkaMessage[T]](actor, TheEnd)
  }
}
