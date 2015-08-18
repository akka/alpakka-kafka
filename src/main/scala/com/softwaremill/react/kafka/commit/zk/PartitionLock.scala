package com.softwaremill.react.kafka.commit.zk

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
private[zk] trait PartitionLock {
  def acquire(): Unit
  def release(): Unit
}
