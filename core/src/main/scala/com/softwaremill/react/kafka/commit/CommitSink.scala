package com.softwaremill.react.kafka.commit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.Sink
import com.softwaremill.react.kafka.ConsumerProperties
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.TheEnd
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * This should become deprecated as soon as we remove KafkaActorSubscriber/KafkaActorPublisher and related stuff.
  */
private[kafka] object CommitSink {

  def create[K, V](
    consumerActor: ActorRef,
    consumerProperties: ConsumerProperties[_, _],
    customDispatcherName: Option[String] = None
  )(implicit actorSystem: ActorSystem) = {
    val initialProps = Props(new ConsumerCommitter(consumerActor, consumerProperties))
    val props = customDispatcherName.map(initialProps.withDispatcher).getOrElse(initialProps)
    val actor = actorSystem.actorOf(props)
    KafkaSink(Sink.actorRef[ConsumerRecord[K, V]](actor, TheEnd), actor)
  }
}

case class KafkaSink[T](sink: Sink[T, Unit], underlyingCommitterActor: ActorRef)