package com.softwaremill.react.kafka.commit

import com.softwaremill.react.kafka.commit.native.NativeCommitterFactory
import kafka.consumer.KafkaConsumer

class CommitterProvider {

  def create[T](kafkaConsumer: KafkaConsumer[T]): Either[CommitterCreationError, OffsetCommitter] = {
    val factoryClass = if (kafkaConsumer.kafkaOffsetStorage) classOf[NativeCommitterFactory].getName
    else {
      "com.softwaremill.react.kafka.commit.zk.ZkCommitterFactory"
    }
    val factory = Class.forName(factoryClass).newInstance().asInstanceOf[CommitterFactory]
    factory.create(kafkaConsumer)
  }
}
