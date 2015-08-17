package com.softwaremill.react.kafka.commit

case class OffsetMap(map: Offsets = Map.empty) {

  def lastOffset(topicPartition: TopicPartition) = map.getOrElse(topicPartition, -1L)

  def diff(other: OffsetMap) =
    OffsetMap((map.toSet diff other.map.toSet).toMap)

  def plusOffset(topicPartition: TopicPartition, offset: Long) =
    copy(map = map + (topicPartition -> offset))

  def nonEmpty = map.nonEmpty
}

object OffsetMap {
  def apply() = new OffsetMap()
}
