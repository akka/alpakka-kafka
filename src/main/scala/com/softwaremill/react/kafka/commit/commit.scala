package com.softwaremill.react.kafka

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
package object commit {

  import org.apache.curator.utils.ZKPaths

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]
  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type Commit = (OffsetMerge) => OffsetMap

  private[commit] val defaultMerge: OffsetMerge = { case (theirs, ours) => ours }
  private[commit] val defaultOffsets = Map[TopicPartition, Long]()
  private[commit] val defaultCommit: Commit = { merge: OffsetMerge =>
    merge(defaultOffsets, defaultOffsets)
  }

  private[commit] def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)
}
