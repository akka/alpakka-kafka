package com.softwaremill.react.kafka.benchmarks

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.commit.{KafkaCommitterSink, OffsetMap, CommitSink}
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

  def consumerPropsFrequentCommit_randomGroup(f: Fixture) = {
    ConsumerProperties(f.host, f.topic, ReactiveKafkaBenchmark.uuid(), new StringDeserializer(), new StringDeserializer())
      .noAutoCommit()
      .commitInterval(1 second)
  }

  def actorSourceProviderNoCommit(system: ActorSystem) = actorSourceProvider(system, consumerPropsNoCommit_randomGroup) _

  def actorSourceProvider(system: ActorSystem, propsProvider: Fixture => ConsumerProperties[String, String])(f: Fixture) = {
    val actorConsumerProps = propsProvider(f)
    println(s"Creating actor for consumer properties: $actorConsumerProps")
    val actorConsumer = ReactiveKafkaConsumer(actorConsumerProps)
    val actorSourceProps = Props(new KafkaActorPublisher(actorConsumer))
    val actorPublisher = ActorPublisher[ConsumerRecord[String, String]](system.actorOf(actorSourceProps))
    (Source.fromPublisher(actorPublisher), actorConsumer)
  }

  def actorSourceProviderWithCommitSink(system: ActorSystem, propsProvider: Fixture => ConsumerProperties[String, String])(f: Fixture) = {
    val actorConsumerProps = propsProvider(f)
    println(s"Creating actor for consumer properties: $actorConsumerProps")
    val actorConsumer = ReactiveKafkaConsumer(actorConsumerProps)
    val actorSourceProps = Props(new KafkaActorPublisher(actorConsumer))
    val consumerActor: ActorRef = system.actorOf(actorSourceProps)
    val actorPublisher = ActorPublisher[ConsumerRecord[String, String]](consumerActor)
    val sink: Sink[ConsumerRecord[String, String], Unit] = CommitSink.create(consumerActor, actorConsumerProps)(system).sink
    (Source.fromPublisher(actorPublisher), actorConsumer, sink)
  }

  def graphSourceProviderNoCommit = graphSourceProvider(consumerPropsNoCommit_randomGroup) _

  def graphSourceProvider(propsProvider: Fixture => ConsumerProperties[String, String])(f: Fixture) = {
    val graphConsumerProps = propsProvider(f)
    println(s"Creating commit sink and graph source for consumer properties: $graphConsumerProps")
    val graphConsumer = ReactiveKafkaConsumer(graphConsumerProps)
    (Source.fromGraph(new KafkaGraphStageSource(graphConsumer)), graphConsumer)
  }

  def graphSourceProviderCommitSink(propsProvider: Fixture => ConsumerProperties[String, String])(f: Fixture) = {
    val graphConsumerProps = propsProvider(f)
    val offsetMap = OffsetMap()
    val offsetSink: Sink[ConsumerRecord[String, String], Unit] = Sink.fromGraph(new KafkaCommitterSink(graphConsumerProps, offsetMap))
    println(s"Creating commit sink and graph source for consumer properties: $graphConsumerProps")
    val graphConsumer = ReactiveKafkaConsumer(graphConsumerProps)
    (Source.fromGraph(new KafkaGraphStageSource(graphConsumer)), graphConsumer, offsetSink)
  }

  def graphSourceProviderWithCommit = graphSourceProviderCommitSink(consumerPropsFrequentCommit_randomGroup) _

  def actorSourceProviderWithCommit(system: ActorSystem) = actorSourceProviderWithCommitSink(system, consumerPropsFrequentCommit_randomGroup) _

}
