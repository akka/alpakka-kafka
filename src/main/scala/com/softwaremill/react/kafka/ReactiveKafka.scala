package com.softwaremill.react.kafka

import kafka.consumer.KafkaConsumer
import kafka.producer.KafkaProducer
import org.reactivestreams.{Publisher, Subscriber}

class ReactiveKafka(host: String, zooKeeperHost: String) {

  def publish(topic: String, groupId: String): Subscriber[String] = {
    val producer = new KafkaProducer(topic, host)
    new ReactiveKafkaSubscriber(producer)
  }

  def consume(topic: String, groupId: String): Publisher[String] = {
    val consumer = new KafkaConsumer(topic, groupId, zooKeeperHost)
    new ReactiveKafkaPublisher(consumer)
  }
}





