package com.softwaremill.react.kafka

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.stream.actor.{ActorSubscriber, ActorPublisher}
import kafka.consumer._
import kafka.producer._
import kafka.serializer.{Encoder, Decoder}
import org.reactivestreams.{Publisher, Subscriber}

class ReactiveKafka(val host: String, val zooKeeperHost: String) {

  def publish[T](
    topic: String,
    groupId: String,
    encoder: Encoder[T],
    partitionizer: T => Option[Array[Byte]] = (_: T) => None
  )(implicit actorSystem: ActorSystem): Subscriber[T] = {
    val props = ProducerProps(host, topic, groupId)
    ActorSubscriber[T](producerActor(props, encoder, partitionizer))
  }

  def publish[T](props: ProducerProps, encoder: Encoder[T])(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(props, encoder))
  }

  def producerActor[T](topic: String, groupId: String, encoder: Encoder[T])(implicit actorSystem: ActorSystem): ActorRef = {
    val props = ProducerProps(host, topic, groupId)
    producerActor(props, encoder)
  }

  def producerActor[T](
    props: ProducerProps,
    encoder: Encoder[T],
    partitionizer: T => Option[Array[Byte]] = (_: T) => None
  )(implicit actorSystem: ActorSystem): ActorRef = {
    val producer = new KafkaProducer(props)
    actorSystem.actorOf(Props(new KafkaActorSubscriber(producer, encoder, partitionizer)).withDispatcher("kafka-subscriber-dispatcher"))
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
