package com.softwaremill.react.kafka

import org.apache.kafka.common.TopicPartition

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
package object commit {

  type Offsets = Map[TopicPartition, Long]
  type OffsetMerge = (Offsets, Offsets) => Offsets

  private[commit] val defaultMerge: OffsetMerge = { case (theirs, ours) => ours }
  private[commit] val defaultOffsets = Map[TopicPartition, Long]()

}
