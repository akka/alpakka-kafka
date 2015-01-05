package com.softwaremill.react.kafka

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.stream.actor.ActorPublisher
import kafka.consumer.KafkaConsumer
import kafka.producer.KafkaProducer
import org.reactivestreams.{Publisher, Subscriber}

class ReactiveKafka(val host: String, val zooKeeperHost: String) {

  def publish(topic: String, groupId: String): Subscriber[String] = {
    val producer = new KafkaProducer(topic, host)
    new ReactiveKafkaSubscriber(producer)
  }

  def consume(topic: String, groupId: String, actorSystem: ActorSystem): Publisher[String] = {
    ActorPublisher[String](consumeAsActor(topic, groupId, actorSystem))
  }

  def consumeAsActor(topic: String, groupId: String, actorSystem: ActorSystem): ActorRef = {
    val consumer = new KafkaConsumer(topic, groupId, zooKeeperHost)
    actorSystem.actorOf(Props(new KafkaActorPublisher(consumer)))
  }

}





