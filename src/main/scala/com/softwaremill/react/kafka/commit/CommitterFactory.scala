package com.softwaremill.react.kafka.commit

import kafka.consumer.KafkaConsumer
import scala.util.Try

trait CommitterFactory {
  def create(kafkaConsumer: KafkaConsumer[_]): Try[OffsetCommitter]
}
