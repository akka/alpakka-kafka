package com.cj.kafka.rx

trait PartitionLock {
  def acquire(): Unit
  def release(): Unit
}

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

trait OffsetCommitter {
  def commit(offsets: OffsetMap, userMerge: OffsetMerge): OffsetMap

  // optional / default fns below
  def start(): Unit = ()
  def stop(): Unit = ()
}
