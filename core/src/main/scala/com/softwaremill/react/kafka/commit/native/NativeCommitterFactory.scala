package com.softwaremill.react.kafka.commit.native

import com.softwaremill.react.kafka.commit._
import kafka.api.{ConsumerMetadataRequest, ConsumerMetadataResponse}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel

import scala.util.{Failure, Success, Try}

class NativeCommitterFactory extends CommitterFactory {

  lazy val offsetManagerResolver = new OffsetManagerResolver

  override def create(kafkaConsumer: KafkaConsumer[_]) = {
    for {
      channel <- offsetManagerResolver.resolve(kafkaConsumer)
    } yield new NativeCommitter(kafkaConsumer, offsetManagerResolver, channel)
  }

}