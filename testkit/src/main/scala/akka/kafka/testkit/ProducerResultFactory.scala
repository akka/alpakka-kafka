/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit

import akka.annotation.ApiMayChange
import akka.kafka.ProducerMessage
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.collection.immutable
import scala.collection.JavaConverters._

/**
 * Factory methods to create instances that normally are emitted by [[akka.kafka.scaladsl.Producer]] and [[akka.kafka.javadsl.Producer]] flows.
 */
@ApiMayChange
object ProducerResultFactory {

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

  /** Java API */
  def multiResult[K, V, PassThrough](
      parts: java.util.Collection[ProducerMessage.MultiResultPart[K, V]],
      passThrough: PassThrough
  ): ProducerMessage.MultiResult[K, V, PassThrough] = ProducerMessage.MultiResult(parts.asScala.toList, passThrough)
}
