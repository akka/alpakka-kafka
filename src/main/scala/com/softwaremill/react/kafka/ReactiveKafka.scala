package com.softwaremill.react.kafka

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import kafka.consumer._
import kafka.producer._
import kafka.serializer.{Decoder, Encoder}
import org.reactivestreams.{Publisher, Subscriber}

class ReactiveKafka(val host: String = "", val zooKeeperHost: String = "") {

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
    val props = ProducerProps(host, topic, groupId, encoder, partitionizer: T => Option[Array[Byte]])
    producerActor(props)
  }

  def publish[T](
    props: ProducerProps[T],
    requestStrategy: () => RequestStrategy
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(props, requestStrategy))
  }

  def publish[T](
    props: ProducerProps[T]
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(props))
  }

  def producerActor[T](
    props: ProducerProps[T],
    requestStrategy: () => RequestStrategy
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(producerActorProps(props, requestStrategy).withDispatcher("kafka-subscriber-dispatcher"))
  }

  def producerActorProps[T](
    props: ProducerProps[T],
    requestStrategy: () => RequestStrategy
  ) = {
    val producer = new KafkaProducer(props)
    Props(
      new KafkaActorSubscriber[T](producer, props, requestStrategy)
    )
  }

  def producerActor[T](
    props: ProducerProps[T]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, () => WatermarkRequestStrategy(10))
  }

  @deprecated("Use ConsumerProps", "0.7.0")
  def consume[T](
    topic: String,
    groupId: String,
    decoder: Decoder[T]
  )(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeAsActor(topic, groupId, decoder))
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
  def consumeAsActor[T](
    topic: String,
    groupId: String,
    decoder: Decoder[T]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ConsumerProps(host, zooKeeperHost, topic, groupId, decoder)
    consumeAsActor(props)
  }

  @deprecated("Use ConsumerProps", "0.7.0")
  def consumeFromEndAsActor[T](
    topic: String,
    groupId: String,
    decoder: Decoder[T]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ConsumerProps(host, zooKeeperHost, topic, groupId, decoder).readFromEndOfStream()
    consumeAsActor(props)
  }

  def consume[T](
    props: ConsumerProps[T]
  )(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeAsActor(props))
  }

  def consumeAsActor[T](props: ConsumerProps[T])(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(consumerActorProps(props).withDispatcher("kafka-publisher-dispatcher"))
  }

  def consumerActorProps[T](props: ConsumerProps[T]) = {
    val consumer = new KafkaConsumer(props)
    Props(new KafkaActorPublisher(consumer))
  }

}