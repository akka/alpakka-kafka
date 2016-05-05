package com.softwaremill.react.kafka.commit

import kafka.common.{OffsetAndMetadata, TopicAndPartition}

import scala.collection.immutable

case class OffsetMap(map: Offsets = Map.empty) {

  def lastOffset(topicPartition: TopicAndPartition) = map.getOrElse(topicPartition, -1L)

  def diff(other: OffsetMap) =
    OffsetMap((map.toSet diff other.map.toSet).toMap)

  def intersect(other: OffsetMap) =
    OffsetMap((map.toSet intersect other.map.toSet).toMap)

  def plusOffset(topicPartition: TopicAndPartition, offset: Long) =
    copy(map = map + (topicPartition -> offset))

  def updateOffset(other: OffsetMap) =
    copy(map = map ++ other.map)

  def nonEmpty = map.nonEmpty

  def toFetchRequestInfo = map.keys.toSeq

  def toCommitRequestInfo = {
    val now = System.currentTimeMillis()
    // Kafka expects the offset of the first unfetched message, and we have the
    // offset of the last fetched message
    map.mapValues(offset => OffsetAndMetadata(offset + 1, timestamp = now))
  }
}

object OffsetMap {
  def apply() = new OffsetMap()

}
