package com.softwaremill.react.kafka

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.stream.actor.{WatermarkRequestStrategy, RequestStrategy, ActorSubscriber, ActorPublisher}
import kafka.consumer._
import kafka.producer._
import kafka.serializer.{Encoder, Decoder}
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
    val producer = new KafkaProducer(props)
    actorSystem.actorOf(Props(
      new KafkaActorSubscriber[T](producer, props, requestStrategy)
    ).withDispatcher("kafka-subscriber-dispatcher"))
  }

  def producerActor[T](
    props: ProducerProps[T]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, () => WatermarkRequestStrategy(10))
  }

  def consume[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeAsActor(topic, groupId, decoder))
  }

  def consumeFromEnd[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeFromEndAsActor(topic, groupId, decoder))
  }

  def consume[T](props: ConsumerProps, decoder: Decoder[T])(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeAsActor(props, decoder))
  }

  def consumeAsActor[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ConsumerProps(host, zooKeeperHost, topic, groupId)
    consumeAsActor(props, decoder)
  }

  def consumeFromEndAsActor[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ConsumerProps(host, zooKeeperHost, topic, groupId).readFromEndOfStream()
    consumeAsActor(props, decoder)
  }

  def consumeAsActor[T](props: ConsumerProps, decoder: Decoder[T])(implicit actorSystem: ActorSystem): ActorRef = {
    val consumer = new KafkaConsumer(props)
    actorSystem.actorOf(Props(new KafkaActorPublisher(consumer, decoder)).withDispatcher("kafka-publisher-dispatcher"))
  }
}