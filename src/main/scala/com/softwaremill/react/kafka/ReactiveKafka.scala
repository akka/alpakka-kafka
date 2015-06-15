package com.softwaremill.react.kafka

import akka.actor.{ ActorRef, Props, ActorSystem }
import akka.stream.actor.{ ActorSubscriber, ActorPublisher }
import kafka.consumer.KafkaConsumer
import kafka.producer.KafkaProducer
import kafka.serializer.{ Encoder, Decoder }
import org.reactivestreams.{ Publisher, Subscriber }

class ReactiveKafka(val host: String, val zooKeeperHost: String) {

  def publish[T](
    topic: String,
    groupId: String,
    encoder: Encoder[T],
    partitionizer: T => Option[Array[Byte]] = (_: T) => None)(implicit actorSystem: ActorSystem): Subscriber[T] = {
    ActorSubscriber[T](producerActor(topic, groupId, encoder, partitionizer))
  }

  def producerActor[T](
    topic: String,
    groupId: String,
    encoder: Encoder[T],
    partitionizer: T => Option[Array[Byte]] = (_: T) => None)(implicit actorSystem: ActorSystem): ActorRef = {
    val producer = new KafkaProducer(topic, host)
    actorSystem.actorOf(Props(new KafkaActorSubscriber(producer, encoder, partitionizer)).withDispatcher("kafka-subscriber-dispatcher"))
  }

  def consume[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeAsActor(topic, groupId, decoder))
  }

  def consumeFromEnd[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): Publisher[T] = {
    ActorPublisher[T](consumeFromEndAsActor(topic, groupId, decoder))
  }

  def consumeAsActor[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): ActorRef = {
    val consumer = new KafkaConsumer(topic, groupId, zooKeeperHost)
    actorSystem.actorOf(Props(new KafkaActorPublisher(consumer, decoder)).withDispatcher("kafka-publisher-dispatcher"))
  }

  def consumeFromEndAsActor[T](topic: String, groupId: String, decoder: Decoder[T])(implicit actorSystem: ActorSystem): ActorRef = {
    val consumer = new KafkaConsumer(topic, groupId, zooKeeperHost, readFromStartOfStream = false)
    actorSystem.actorOf(Props(new KafkaActorPublisher(consumer, decoder)).withDispatcher("kafka-publisher-dispatcher"))
  }

}





