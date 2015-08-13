package com.softwaremill.react.kafka.commit

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
trait PartitionLock {
  def acquire(): Unit
  def release(): Unit
}
