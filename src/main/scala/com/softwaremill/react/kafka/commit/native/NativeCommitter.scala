package com.softwaremill.react.kafka.commit.native

import com.softwaremill.react.kafka.commit.{OffsetCommitter, OffsetMap}
import kafka.api.{OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse}
import kafka.common.ErrorMapping.{ConsumerCoordinatorNotAvailableCode, NotCoordinatorForConsumerCode, OffsetsLoadInProgressCode}
import kafka.common.{ErrorMapping, OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel

import scala.util.{Failure, Success, Try}

/**
 * NOT THREAD SAFE.
 */
private[native] class NativeCommitter(
    kafkaConsumer: KafkaConsumer[_],
    offsetManagerResolver: OffsetManagerResolver,
    var channel: BlockingChannel
) extends OffsetCommitter {

  var correlationId = 0

  override def commit(offsets: OffsetMap): Try[OffsetMap] = commitTrial(offsets)

  private def commitTrial(
    offsetsToCommit: OffsetMap,
    retriesLeft: Int = NativeCommitter.MaxRetries,
    lastErrorOpt: Option[Throwable] = None
  ) = {
    if (retriesLeft == 0)
      Failure(lastErrorOpt.map(new NativeCommitFailedException(_)).getOrElse(new IllegalStateException())) // TODO remove the default exception
    else {
      val commitRequest = createCommitRequest(offsetsToCommit.toCommitRequestInfo)
      sendCommit(commitRequest).flatMap { response =>
        if (response.hasError)
          handleCommitError(offsetsToCommit, retriesLeft, response)
        else
          getOffsetsTrial(offsetsToCommit)
      }
    }
  }

  private def handleCommitError(offsets: OffsetMap, retriesLeft: Int, commitResponse: OffsetCommitResponse): Try[OffsetMap] = {
    if (offsetManagerHasMoved(commitResponse))
      findNewCoordinatorAndRetry(offsets, retriesLeft, commitTrial)
    else
      retry(offsets, retriesLeft, new KafkaErrorException(commitResponse.commitStatus), commitTrial)
  }

  private def createCommitRequest(requestInfo: Map[TopicAndPartition, OffsetAndMetadata]): OffsetCommitRequest = {
    correlationId = correlationId + 1
    new OffsetCommitRequest(
      kafkaConsumer.props.groupId,
      requestInfo,
      versionId = OffsetCommitRequest.CurrentVersion,
      correlationId = correlationId
    )
  }

  private def sendCommit(commitRequest: OffsetCommitRequest): Try[OffsetCommitResponse] = {
    Try {
      channel.send(commitRequest)
      OffsetCommitResponse.readFrom(channel.receive().buffer)
    }
  }

  private def getOffsetsTrial(
    offsetsToVerify: OffsetMap,
    retriesLeft: Int = NativeCommitter.MaxRetries,
    lastErrorOpt: Option[Throwable] = None
  ): Try[OffsetMap] = {

    if (retriesLeft == 0)
      Failure(lastErrorOpt.map(new NativeCommitFailedException(_)).getOrElse(new IllegalStateException())) // TODO remove the default exception
    else {
      sendFetch(offsetsToVerify).flatMap {
        response =>
          val result = response.requestInfo
          if (hasError(response))
            handleFetchError(response, offsetsToVerify, retriesLeft)
          else
            Success(OffsetMap(result.mapValues(_.offset)))
      }
    }
  }

  private def sendFetch(offsetsToVerify: OffsetMap) = {
    Try {
      channel.send(createFetchRequest(offsetsToVerify))
      OffsetFetchResponse.readFrom(channel.receive().buffer)
    }
  }

  private def createFetchRequest(offsetsToVerify: OffsetMap): OffsetFetchRequest = {
    new OffsetFetchRequest(
      kafkaConsumer.props.groupId,
      offsetsToVerify.toFetchRequestInfo,
      versionId = OffsetCommitRequest.CurrentVersion,
      correlationId = correlationId
    )
  }

  private def handleFetchError(
    fetchResponse: OffsetFetchResponse,
    offsetsToVerify: OffsetMap,
    retriesLeft: Int
  ): Try[OffsetMap] = {
    if (offsetManagerHasMoved(fetchResponse))
      findNewCoordinatorAndRetry(offsetsToVerify, retriesLeft, getOffsetsTrial)
    else if (offsetLoadInProgress(fetchResponse))
      retry(offsetsToVerify, retriesLeft, OfssetLoadInProgressException, getOffsetsTrial)
    else
      retry(
        offsetsToVerify,
        retriesLeft,
        new KafkaErrorException(fetchResponse.requestInfo.mapValues(_.error)),
        commitTrial
      )
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

  private def hasError(offsetFetchResponse: OffsetFetchResponse) = {
    offsetFetchResponse.requestInfo.exists{ case (topicAndPartition, meta) => meta.error != ErrorMapping.NoError }
  }

  private def findNewCoordinatorAndRetry(
    offsetsToVerify: OffsetMap,
    retriesLeft: Int,
    funToRetry: (OffsetMap, Int, Option[Throwable]) => Try[OffsetMap]
  ): Try[OffsetMap] = {
    channel.disconnect()
    for {
      newChannel <- offsetManagerResolver.resolve(kafkaConsumer)
      offsetMap <- retry(offsetsToVerify, retriesLeft, noCoordinatorException, funToRetry)
    } yield {
      this.channel = newChannel
      offsetMap
    }
  }

  private def noCoordinatorException = new CoordinatorNotFoundException(kafkaConsumer.props.brokerList)

  private def retry(
    offsets: OffsetMap,
    retriesLeft: Int,
    cause: Throwable,
    funToRetry: (OffsetMap, Int, Option[Throwable]) => Try[OffsetMap]
  ) = {
    Thread.sleep(NativeCommitter.RetryIntervalMs)
    funToRetry(offsets, retriesLeft - 1, Some(cause))
  }

  override def stop(): Unit = {
    channel.disconnect()
    super.stop()
  }
}

object NativeCommitter {
  val MaxRetries = 5
  val RetryIntervalMs: Long = 200
}

private[native] class NativeCommitFailedException(cause: Throwable)
  extends Exception(s"Failed to commit after ${NativeCommitter.MaxRetries} retires", cause)

private[native] object OfssetLoadInProgressException
  extends Exception("Cannot fetch offsets, coordinator responded with OffsetsLoadInProgressCode")

private[native] class NativeGetOffsetsFailedException(cause: Throwable)
  extends Exception(s"Failed to fetch offset data after ${NativeCommitter.MaxRetries} retires", cause)

private[native] class CoordinatorNotFoundException(brokerList: String)
  extends Exception(s"Could not find offset manager in broker list: $brokerList")

private[native] class KafkaErrorException(statuses: Map[TopicAndPartition, Short])
  extends Exception(s"Received statuses: $statuses")