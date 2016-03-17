/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka_api_opt3

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import akka.stream.Inlet
import org.apache.kafka.common.TopicPartition
import akka.stream.stage.GraphStageWithMaterializedValue
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import akka.stream.Outlet
import org.apache.kafka.clients.consumer.ConsumerRecord
import akka.stream.SourceShape
import org.apache.kafka.clients.consumer.KafkaConsumer
import akka.stream.Shape
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Flow
import scala.concurrent.Future
import akka.NotUsed
import akka.Done

object Consumer {

  trait Message[K, V] {
    def transaction: Transaction
    def record: ConsumerRecord[K, V]
  }

  trait Transaction {
    def commit(): Future[Done]
  }

  trait Control {
    def stop(): Unit
  }

  def source[K, V](consumerProvider: () => KafkaConsumer[K, V]): Source[Message[K, V], Control] = ???

}

