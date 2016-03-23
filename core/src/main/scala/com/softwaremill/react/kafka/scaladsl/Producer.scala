/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka.scaladsl

import scala.concurrent.Future
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object Producer {

  final case class Message[K, V, PassThrough](
    record: ProducerRecord[K, V],
    passThrough: PassThrough)

  final case class Result[K, V, PassThrough](
    offset: Long,
    message: Message[K, V, PassThrough])

  /**
   * Publish records to Kafka topics.
   */
  def plainSink[K, V](producerProvider: () => KafkaProducer[K, V]): Sink[ProducerRecord[K, V], NotUsed] = ???

  /**
   * Sink that is aware of the committable offset from a [[Consumer]]. It will commit the consumer offset
   * when the message has been published successfully to the topic.
   */
  def commitableOffsetSink[K, V](producerProvider: () => KafkaProducer[K, V]): Sink[Message[K, V, Consumer.CommittableOffset], NotUsed] = ???

  /**
   * Publish records to Kafka topics and then continue the flow. Possibility to pass through a message, which
   * can for example be a [[Consumer.CommittableOffset]] that can be committed later in the flow.
   */
  def flow[K, V, PassThrough](producerProvider: () => KafkaProducer[K, V]): Flow[Message[K, V, PassThrough], Future[Result[K, V, PassThrough]], NotUsed] =
    ???

}

