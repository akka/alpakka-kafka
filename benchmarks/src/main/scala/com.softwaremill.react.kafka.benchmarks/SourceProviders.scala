package com.softwaremill.react.kafka.benchmarks

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.commit.{KafkaCommitterSink, OffsetMap}
import com.softwaremill.react.kafka.{ConsumerProperties, KafkaGraphStageSource, ReactiveKafkaConsumer}
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
    val offsetSink: Sink[ConsumerRecord[String, String], NotUsed] = Sink.fromGraph(new KafkaCommitterSink(graphConsumerProps, offsetMap))
    println(s"Creating commit sink and graph source for consumer properties: $graphConsumerProps")
    val graphConsumer = ReactiveKafkaConsumer(graphConsumerProps)
    (Source.fromGraph(new KafkaGraphStageSource(graphConsumer)), graphConsumer, offsetSink)
  }

  def graphSourceProviderWithCommit = graphSourceProviderCommitSink(consumerPropsFrequentCommit_randomGroup) _
}
