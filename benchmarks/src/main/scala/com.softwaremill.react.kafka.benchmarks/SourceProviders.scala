package com.softwaremill.react.kafka.benchmarks

import akka.actor.{ActorSystem, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import com.softwaremill.react.kafka.{ConsumerProperties, KafkaActorPublisher, KafkaGraphStageSource, ReactiveKafkaConsumer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import scala.concurrent.duration._
import scala.language.postfixOps

trait SourceProviders {

  def consumerPropsNoCommit_randomGroup(f: Fixture) = {
    ConsumerProperties(f.host, f.topic, ReactiveKafkaBenchmark.uuid(), new StringDeserializer(), new StringDeserializer())
      .noAutoCommit()
      .commitInterval(200 days)
  }

  def actorSourceProvider(system: ActorSystem)(f: Fixture) = {
    val actorConsumerProps = consumerPropsNoCommit_randomGroup(f)
    println(s"Creating actor for consumer properties: $actorConsumerProps")
    val actorConsumer = ReactiveKafkaConsumer(actorConsumerProps)
    val actorSourceProps = Props(new KafkaActorPublisher(actorConsumer))
    val actorPublisher = ActorPublisher[ConsumerRecord[String, String]](system.actorOf(actorSourceProps))
    (Source.fromPublisher(actorPublisher), actorConsumer)
  }

  def graphSourceProvider(f: Fixture) = {
    val graphConsumerProps = consumerPropsNoCommit_randomGroup(f)
    println(s"Creating graph source for consumer properties: $graphConsumerProps")
    val graphConsumer = ReactiveKafkaConsumer(graphConsumerProps)
    (Source.fromGraph(new KafkaGraphStageSource(graphConsumer)), graphConsumer)
  }

}
