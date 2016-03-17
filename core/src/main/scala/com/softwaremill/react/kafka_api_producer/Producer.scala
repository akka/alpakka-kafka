/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka_api_producer

import scala.concurrent.Future
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import com.softwaremill.react.kafka_api_opt3.Consumer

object Producer {

  def flow[K, V](producerProvider: () => KafkaProducer[K, V]): Flow[ProducerRecord[K, V], Future[ProducerRecord[K, V]], NotUsed] =
    ???

  def sink[K, V](producerProvider: () => KafkaProducer[K, V]): Sink[ProducerRecord[K, V], NotUsed] =
    flow(producerProvider).to(Sink.ignore)

  /**
   * This is for integration with opt3 Consumer
   */
  def sinkWithConsumerTransaction[K, V](producerProvider: () => KafkaProducer[K, V]): Sink[(ProducerRecord[K, V], Consumer.Transaction), NotUsed] = ???

  /**
   * This is an alternative to `flow` and `sinkWithConsumerTransaction`, the `DoneMsg` can carry the `Transaction`.
   */
  def flow2[K, V, DoneMsg](producerProvider: () => KafkaProducer[K, V]): Flow[(ProducerRecord[K, V], DoneMsg), Future[DoneMsg], NotUsed] = ???
}

