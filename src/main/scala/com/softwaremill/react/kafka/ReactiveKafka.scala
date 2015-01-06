package com.softwaremill.react.kafka

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.stream.actor.{ActorSubscriber, ActorPublisher}
import kafka.consumer.KafkaConsumer
import kafka.producer.KafkaProducer
import org.reactivestreams.{Publisher, Subscriber}

class ReactiveKafka(val host: String, val zooKeeperHost: String) {

  def publish(topic: String, groupId: String)(implicit actorSystem: ActorSystem): Subscriber[String] = {
    ActorSubscriber[String](producerActor(topic, groupId))
  }

  def producerActor(topic: String, groupId: String)(implicit actorSystem: ActorSystem): ActorRef = {
    val producer = new KafkaProducer(topic, host)
    actorSystem.actorOf(Props(new KafkaActorSubscriber(producer)))
  }

  def consume(topic: String, groupId: String)(implicit actorSystem: ActorSystem): Publisher[String] = {
    ActorPublisher[String](consumeAsActor(topic, groupId))
  }

  def consumeAsActor(topic: String, groupId: String)(implicit actorSystem: ActorSystem): ActorRef = {
    val consumer = new KafkaConsumer(topic, groupId, zooKeeperHost)
    actorSystem.actorOf(Props(new KafkaActorPublisher(consumer)))
  }

}





