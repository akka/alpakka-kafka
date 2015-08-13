package com.cj.kafka

package object rx {

  import org.apache.curator.utils.ZKPaths

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]
  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type Commit = (OffsetMerge) => OffsetMap

  private[rx] val defaultMerge: OffsetMerge = { case (theirs, ours) => ours }
  private[rx] val defaultOffsets = Map[TopicPartition, Long]()
  private[rx] val defaultCommit: Commit = { merge: OffsetMerge =>
    merge(defaultOffsets, defaultOffsets)
  }

  private[rx] def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)
}
