package com.softwaremill.react.kafka.commit.native

import com.softwaremill.react.kafka.commit.{OffsetMap, OffsetCommitter}
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel

private[native] class NativeCommitter(
    kafkaConsumer: KafkaConsumer[_],
    channel: BlockingChannel
) extends OffsetCommitter {

  override def commit(offsets: OffsetMap): OffsetMap = {
    ???
  }

  override def stop(): Unit = {
    channel.disconnect()
    super.stop()
  }
}
