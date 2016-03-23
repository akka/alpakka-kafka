/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

object Consumer {

  final case class Message[K, V](
    key: K,
    value: V,
    offset: Offset)

  final case class CommitableMessage[K, V](
    key: K,
    value: V,
    committableOffset: CommittableOffset)

  trait CommittableOffset {
    def commit(): Future[Done]
    def offset: Offset
  }

  final case class Offset(topic: String, partition: Int, offset: Long)

  trait Control {
    def stop(): Future[Done]
  }

  /**
   * No support for committing offsets to Kafka. Can be used when offset is stored externally or with auto-commit.
   */
  def plainSource[K, V](consumerProvider: () => KafkaConsumer[K, V]): Source[Message[K, V], Control] = ???

  /**
   * Offset position can be committed to Kafka.
   */
  def committableSource[K, V](consumerProvider: () => KafkaConsumer[K, V]): Source[CommitableMessage[K, V], Control] = ???

  /**
   * Commits offset position for each message immediately before delivering it.
   * Used for at-most-once delivery semantics.
   */
  def atMostOnceSource[K, V](consumerProvider: () => KafkaConsumer[K, V]): Source[Message[K, V], Control] = ???

}

