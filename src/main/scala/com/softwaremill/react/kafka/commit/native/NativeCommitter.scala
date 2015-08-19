package com.softwaremill.react.kafka.commit.native

import com.softwaremill.react.kafka.commit.{OffsetCommitter, OffsetMap}
import kafka.api.{OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse}
import kafka.common.ErrorMapping
import kafka.common.ErrorMapping.{ConsumerCoordinatorNotAvailableCode, NotCoordinatorForConsumerCode, OffsetsLoadInProgressCode}
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel

import scala.util.{Failure, Success, Try}

private[native] class NativeCommitter(
    kafkaConsumer: KafkaConsumer[_],
    channel: BlockingChannel
) extends OffsetCommitter {

  var correlationId = 0

  override def commit(offsets: OffsetMap): OffsetMap = {

    val requestInfo = offsets.toCommitRequestInfo
    correlationId = correlationId + 1
    val commitRequest = new OffsetCommitRequest(
      kafkaConsumer.props.groupId,
      requestInfo,
      versionId = OffsetCommitRequest.CurrentVersion,
      correlationId = correlationId
    ) // version 1 and above commit to Kafka, version 0 commits to ZooKeeper
    Try {
      channel.send(commitRequest)
      val commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer)
      if (commitResponse.hasError)
        if (offsetManagerHasMoved(commitResponse))
          findNewCoordinatorAndRetryCommit()
        else
          throw new IllegalStateException(s"Cannot commit due to errors: ${commitResponse.commitStatus}") // TODO try to reconnect a few times?
    } match {
      case Failure(reason) => throw reason // TODO better error handling?
      case Success(()) => getOffsets(offsets)
    }
  }

  private def getOffsets(offsets: OffsetMap) = {

    val fetchRequest = new OffsetFetchRequest(
      kafkaConsumer.props.groupId,
      offsets.toFetchRequestInfo,
      versionId = OffsetCommitRequest.CurrentVersion,
      correlationId = correlationId
    )
    // TODO exception handling
    channel.send(fetchRequest)
    val fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer)
    val result = fetchResponse.requestInfo
    if (hasError(fetchResponse))
      handleError(fetchResponse)
    else
      OffsetMap(result.mapValues(_.offset))
  }

  private def handleError(fetchResponse: OffsetFetchResponse): OffsetMap = {
    if (offsetManagerHasMoved(fetchResponse))
      findNewCoordinatorAndRetryFetch()
    else if (offsetLoadInProgress(fetchResponse))
      retryFetch()
    else
      handleUnrecoverableError(fetchResponse)
  }

  def handleUnrecoverableError(fetchResponse: OffsetFetchResponse): OffsetMap = {
    throw new IllegalStateException(s"Could not fetch commits. Response: $fetchResponse")
    // TODO
  }

  private def offsetManagerHasMoved(commitResponse: OffsetCommitResponse): Boolean = {
    offsetManagerHasMoved(commitResponse.commitStatus.values)
  }

  private def offsetManagerHasMoved(fetchResponse: OffsetFetchResponse): Boolean = {
    offsetManagerHasMoved(fetchResponse.requestInfo.values.map(_.error))
  }

  private def offsetLoadInProgress(fetchResponse: OffsetFetchResponse): Boolean = {
    fetchResponse.requestInfo.values.map(_.error).exists(_ == OffsetsLoadInProgressCode)
  }

  private def offsetManagerHasMoved(errors: Iterable[Short]): Boolean = {
    errors.exists(
      err => err == NotCoordinatorForConsumerCode || err == ConsumerCoordinatorNotAvailableCode
    )
  }

  private def retryFetch(): OffsetMap = {
    throw new UnsupportedOperationException("Received OffsetsLoadInProgressCode, not supported yet")
    // TODO
  }

  def hasError(offsetFetchResponse: OffsetFetchResponse) = {
    offsetFetchResponse.requestInfo.exists{ case (topicAndPartition, meta) => meta.error != ErrorMapping.NoError }
  }

  def findNewCoordinatorAndRetryFetch(): OffsetMap = {
    channel.disconnect()
    throw new UnsupportedOperationException("Received NotCoordinatorForConsumerCode, not supported yet")
    // TODO
  }

  def findNewCoordinatorAndRetryCommit(): Unit = {
    channel.disconnect()
    throw new UnsupportedOperationException("Received NotCoordinatorForConsumerCode, not supported yet")
    // TODO
  }

  override def stop(): Unit = {
    channel.disconnect()
    super.stop()
  }
}