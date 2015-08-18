package com.softwaremill.react.kafka.commit

import kafka.consumer.KafkaConsumer

sealed trait CommitterCreationError
case object StorageNotSupported extends CommitterCreationError
case class StorageConnectionFailed(reason: Throwable) extends CommitterCreationError

trait CommitterFactory {
  def create[T](kafkaConsumer: KafkaConsumer[T]): Either[CommitterCreationError, OffsetCommitter]
}
