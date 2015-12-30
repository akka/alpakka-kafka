package com.softwaremill.react.kafka.commit

import com.softwaremill.react.kafka.commit.native.NativeCommitterFactory
import kafka.consumer.KafkaConsumer

import scala.util.Try

class CommitterProvider {

  def create[T](kafkaConsumer: KafkaConsumer[T]): Try[OffsetCommitter] = {
    val factoryClass = if (kafkaConsumer.kafkaOffsetStorage) classOf[NativeCommitterFactory].getName
    else
      "com.softwaremill.react.kafka.commit.zk.ZkCommitterFactory"
    Try(Class.forName(factoryClass).newInstance().asInstanceOf[CommitterFactory]).flatMap(_.create(kafkaConsumer))
  }
}
