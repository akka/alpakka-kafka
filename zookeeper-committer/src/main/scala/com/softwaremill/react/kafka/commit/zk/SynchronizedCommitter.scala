package com.softwaremill.react.kafka.commit.zk

import kafka.common.TopicAndPartition

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
private[zk] trait SynchronizedCommitter {
  def getPartitionLock(topicPartition: TopicAndPartition): PartitionLock
  def withPartitionLocks[T](partitions: Iterable[TopicAndPartition])(callback: => T): T = {
    val locks = partitions.map(getPartitionLock)
    try {
      locks.foreach(_.acquire())
      callback
    }
    catch {
      case err: Throwable =>
        err.printStackTrace()
        throw err
    }
    finally {
      locks.foreach(_.release())
    }
  }
}
