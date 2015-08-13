package com.softwaremill.react.kafka.commit

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
trait OffsetCommitter {
  def commit(offsets: OffsetMap): OffsetMap

  // optional / default fns below
  def start(): Unit = ()
  def stop(): Unit = ()
}
