package com.softwaremill.react.kafka.commit.native

import com.softwaremill.react.kafka.commit.{StorageNotSupported, OffsetCommitter, CommitterCreationError, CommitterFactory}
import kafka.consumer.KafkaConsumer

class NativeCommitterFactory extends CommitterFactory {
  override def create[T](kafkaConsumer: KafkaConsumer[T]): Either[CommitterCreationError, OffsetCommitter] =
    Left(StorageNotSupported) // TODO
}
