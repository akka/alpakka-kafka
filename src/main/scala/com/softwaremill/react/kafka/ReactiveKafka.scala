package com.softwaremill.react.kafka

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import com.softwaremill.react.kafka.ReactiveKafka.DefaultRequestStrategy
import kafka.consumer._
import kafka.producer._
import kafka.serializer.{Decoder, Encoder}
import org.reactivestreams.{Publisher, Subscriber}

class ReactiveKafka(val host: String = "", val zooKeeperHost: String = "") {

  /**
   * Constructor without default args
   */
  def this() = {
    this("", "")
  }

  @deprecated("Use ProducerProps", "0.7.0")
  def publish[T](
    topic: String,
    groupId: String,
    encoder: Encoder[T],
    partitionizer: T => Option[Array[Byte]] = (_: T) => None
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(topic, groupId, encoder, partitionizer))
  }

  @deprecated("Use ProducerProps", "0.7.0")
  def producerActor[T](
    topic: String,
    groupId: String,
    encoder: Encoder[T],
    partitionizer: T => Option[Array[Byte]]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ProducerProperties(host, topic, groupId, encoder, partitionizer: T => Option[Array[Byte]])
    producerActor(props)
  }

  def publish[T](
    props: ProducerProperties[T],
    requestStrategy: () => RequestStrategy
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(props, requestStrategy))
  }

  def publish[T](
    props: ProducerProperties[T],
    requestStrategy: () => RequestStrategy,
    dispatcher: String
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(props, requestStrategy, dispatcher))
  }

  def publish[T](
    props: ProducerProperties[T],
    dispatcher: String
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(props, dispatcher))
  }

  def publish[T](
    props: ProducerProperties[T]
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(props))
  }

  def producerActor[T](
    props: ProducerProperties[T],
    requestStrategy: () => RequestStrategy
  )(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, requestStrategy, "kafka-subscriber-dispatcher")
  }

  def producerActor[T](
    props: ProducerProperties[T],
    dispatcher: String
  )(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, DefaultRequestStrategy, dispatcher)
  }

  def producerActor[T](
    props: ProducerProperties[T],
    requestStrategy: () => RequestStrategy,
    dispatcher: String
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(producerActorProps(props, requestStrategy).withDispatcher(dispatcher))
  }

  def producerActorProps[T](
    props: ProducerProperties[T],
    requestStrategy: () => RequestStrategy
  ) = {
    val producer = new KafkaProducer(props)
    Props(
      new KafkaActorSubscriber[T](producer, props, requestStrategy)
    )
  }

  def producerActorProps[T](props: ProducerProperties[T]): Props = {
    producerActorProps(props, DefaultRequestStrategy)
  }

  def producerActor[T](
    props: ProducerProperties[T]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(producerActorProps(props))
  }

  @deprecated("Use ConsumerProps", "0.7.0")
  def consume[T](
    topic: String,
    groupId: String,
    decoder: Decoder[T]
  )(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumerActor(topic, groupId, decoder))
  }

  @deprecated("Use ConsumerProps", "0.7.0")
  def consumeFromEnd[T](
    topic: String,
    groupId: String,
    decoder: Decoder[T]
  )(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeFromEndAsActor(topic, groupId, decoder))
  }

  @deprecated("Use ConsumerProps", "0.7.0")
  def consumerActor[T](
    topic: String,
    groupId: String,
    decoder: Decoder[T]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ConsumerProperties(host, zooKeeperHost, topic, groupId, decoder)
    consumerActor(props)
  }

  @deprecated("Use ConsumerProps", "0.7.0")
  def consumeFromEndAsActor[T](
    topic: String,
    groupId: String,
    decoder: Decoder[T]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ConsumerProperties(host, zooKeeperHost, topic, groupId, decoder).readFromEndOfStream()
    consumerActor(props)
  }

  def consume[T](
    props: ConsumerProperties[T]
  )(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumerActor(props))
  }

  def consume[T](
    props: ConsumerProperties[T],
    dispatcher: String
  )(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumerActor(props, dispatcher))
  }

  def consumerActor[T](props: ConsumerProperties[T])(implicit actorSystem: ActorSystem): ActorRef = {
    consumerActor(props, "kafka-publisher-dispatcher")
  }

  def consumerActor[T](
    props: ConsumerProperties[T],
    dispatcher: String
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(consumerActorProps(props).withDispatcher(dispatcher))
  }

  def consumerActorProps[T](props: ConsumerProperties[T]) = {
    val consumer = new KafkaConsumer(props)
    Props(new KafkaActorPublisher(consumer))
  }

}

object ReactiveKafka {
  val DefaultRequestStrategy = () => WatermarkRequestStrategy(10)
}