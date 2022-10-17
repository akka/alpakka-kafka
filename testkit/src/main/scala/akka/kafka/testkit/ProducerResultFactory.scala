/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit

import akka.annotation.ApiMayChange
import akka.kafka.ProducerMessage
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
 * Factory methods to create instances that normally are emitted by [[akka.kafka.scaladsl.Producer]] and [[akka.kafka.javadsl.Producer]] flows.
 */
@ApiMayChange
object ProducerResultFactory {

  def recordMetadata(msg: ProducerRecord[_, _]): RecordMetadata = {
    // null checks are required on Scala 2.11
    val partition = if (msg.partition == null) 0 else msg.partition.toInt
    val timestamp = if (msg.timestamp == null) 0L else msg.timestamp.toLong
    new RecordMetadata(new TopicPartition(msg.topic, partition), -1, 1, timestamp, 2, 2)
  }

  def recordMetadata(topic: String, partition: Int, offset: Long): RecordMetadata =
    new RecordMetadata(new TopicPartition(topic, partition), offset, 0, 12345L, 2, 2)

  def result[K, V, PassThrough](
      message: ProducerMessage.Message[K, V, PassThrough]
  ): ProducerMessage.Result[K, V, PassThrough] = ProducerMessage.Result(recordMetadata(message.record), message)

  def result[K, V, PassThrough](
      metadata: RecordMetadata,
      message: ProducerMessage.Message[K, V, PassThrough]
  ): ProducerMessage.Result[K, V, PassThrough] = ProducerMessage.Result(metadata, message)

  def multiResultPart[K, V](
      metadata: RecordMetadata,
      record: ProducerRecord[K, V]
  ): ProducerMessage.MultiResultPart[K, V] = ProducerMessage.MultiResultPart(metadata, record)

  def multiResult[K, V, PassThrough](
      parts: immutable.Seq[ProducerMessage.MultiResultPart[K, V]],
      passThrough: PassThrough
  ): ProducerMessage.MultiResult[K, V, PassThrough] = ProducerMessage.MultiResult(parts, passThrough)

  def multiResult[K, V, PassThrough](
      message: ProducerMessage.MultiMessage[K, V, PassThrough]
  ): ProducerMessage.MultiResult[K, V, PassThrough] =
    ProducerResultFactory.multiResult(
      message.records.map(r => ProducerResultFactory.multiResultPart(recordMetadata(r), r)),
      message.passThrough
    )

  /** Java API */
  def multiResult[K, V, PassThrough](
      parts: java.util.Collection[ProducerMessage.MultiResultPart[K, V]],
      passThrough: PassThrough
  ): ProducerMessage.MultiResult[K, V, PassThrough] = ProducerMessage.MultiResult(parts.asScala.toList, passThrough)

  def passThroughResult[K, V, PassThrough](
      passThrough: PassThrough
  ): ProducerMessage.PassThroughResult[K, V, PassThrough] = ProducerMessage.PassThroughResult(passThrough)
}
