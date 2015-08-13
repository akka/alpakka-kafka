package com.softwaremill.react.kafka.commit

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
trait SynchronizedCommitter {
  def getPartitionLock(topicPartition: TopicPartition): PartitionLock
  def withPartitionLocks[T](partitions: Iterable[TopicPartition])(callback: => T): T = {
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
