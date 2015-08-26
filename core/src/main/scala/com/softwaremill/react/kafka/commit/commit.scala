package com.softwaremill.react.kafka

import kafka.common.TopicAndPartition

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
package object commit {

  type Offsets = Map[TopicAndPartition, Long]
  type OffsetMerge = (Offsets, Offsets) => Offsets

  private[commit] val defaultMerge: OffsetMerge = { case (theirs, ours) => ours }
  private[commit] val defaultOffsets = Map[TopicAndPartition, Long]()

}
