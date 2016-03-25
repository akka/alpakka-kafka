package com.softwaremill.react.kafka.commit

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

case class OffsetMap(map: Offsets = Map.empty) {

  def lastOffset(topicPartition: TopicPartition) = map.getOrElse(topicPartition, -1L)

  def diff(other: OffsetMap) =
    OffsetMap((map.toSet diff other.map.toSet).toMap)

  def plusOffset(topicPartition: TopicPartition, offset: Long) =
    copy(map = map + (topicPartition -> offset))

  def nonEmpty = map.nonEmpty

  def toFetchRequestInfo = map.keys.toSeq

  def merge(other: OffsetMap) =
    OffsetMap(map ++ other.map)

  def toCommitRequestInfo = {
    // Kafka expects the offset of the first unfetched message, and we have the
    // offset of the last fetched message
    map.mapValues(offset => new OffsetAndMetadata(offset + 1))
  }
}

object OffsetMap {
  def apply() = new OffsetMap()

}
