package com.softwaremill.react.kafka.commit

import scala.util.Try

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
trait OffsetCommitter {
  def commit(offsets: OffsetMap): Try[OffsetMap]

  // optional / default fns below
  def start(): Unit = ()
  def stop(): Unit = ()
}
