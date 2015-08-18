package com.softwaremill.react.kafka.commit

import kafka.common.TopicAndPartition

case class OffsetMap(map: Offsets = Map.empty) {

  def lastOffset(topicPartition: TopicAndPartition) = map.getOrElse(topicPartition, -1L)

  def diff(other: OffsetMap) =
    OffsetMap((map.toSet diff other.map.toSet).toMap)

  def plusOffset(topicPartition: TopicAndPartition, offset: Long) =
    copy(map = map + (topicPartition -> offset))

  def nonEmpty = map.nonEmpty
}

object OffsetMap {
  def apply() = new OffsetMap()
}
