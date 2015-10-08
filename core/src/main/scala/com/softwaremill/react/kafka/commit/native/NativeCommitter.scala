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
    var channel: BlockingChannel,
    sendCommit: (BlockingChannel, OffsetCommitRequest) => Try[OffsetCommitResponse] = KafkaSendCommit,
    sendFetch: (BlockingChannel, OffsetFetchRequest) => Try[OffsetFetchResponse] = KafkaSendFetch
) extends OffsetCommitter {

  var correlationId = 0

  override def commit(offsets: OffsetMap): Try[OffsetMap] =
    commitPhaseTrial(offsets).flatMap { response =>
      getOffsetsTrial(offsets)
    }

  private def commitPhaseTrial(
    offsetsToCommit: OffsetMap,
    retriesLeft: Int = NativeCommitter.MaxRetries,
    lastErrorOpt: Option[Throwable] = None
  ): Try[OffsetCommitResponse] = {
    if (retriesLeft == 0)
      terminateWithLastError(lastErrorOpt)
    else {
      val commitRequest = createCommitRequest(offsetsToCommit.toCommitRequestInfo)
      val commitResult = sendCommit(channel, commitRequest).flatMap { response =>
        if (response.hasError)
          Failure(new KafkaErrorException(response.commitStatus))
        else
          Success(response)
      }
      commitResult match {
        case Failure(exception) =>
          exception match {
            case e: KafkaErrorException =>
              handleCommitErrorCode(offsetsToCommit, retriesLeft, e)
            case otherException =>
              retry(offsetsToCommit, retriesLeft, otherException, commitPhaseTrial)
          }
        case success => success
      }
    }
  }

  private def handleCommitErrorCode(offsets: OffsetMap, retriesLeft: Int, ex: KafkaErrorException) = {

    if (offsetManagerHasMoved(ex.statuses.values))
      findNewCoordinatorAndRetry(offsets, retriesLeft, commitPhaseTrial)
    else
      retry(offsets, retriesLeft, ex, commitPhaseTrial)
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

  private def getOffsetsTrial(
    offsetsToVerify: OffsetMap,
    retriesLeft: Int = NativeCommitter.MaxRetries,
    lastErrorOpt: Option[Throwable] = None
  ): Try[OffsetMap] = {
    if (retriesLeft == 0)
      terminateWithLastError(lastErrorOpt)
    else {
      val fetchResult = sendFetch(channel, createFetchRequest(offsetsToVerify)).flatMap {
        response =>
          if (hasError(response))
            Failure(new KafkaErrorException(response.requestInfo.mapValues(_.error)))
          else
            Success(response)
      }
      fetchResult match {
        case Failure(exception) =>
          exception match {
            case e: KafkaErrorException => handleFetchErrorCode(offsetsToVerify, retriesLeft, e)
            case otherException => retry(offsetsToVerify, retriesLeft, otherException, getOffsetsTrial)
          }
        case Success(result) => Success(OffsetMap(result.requestInfo.mapValues(_.offset)))
      }
    }
  }

  private def terminateWithLastError(lastErrorOpt: Option[Throwable]) = {
    Failure(lastErrorOpt
      .map(new NativeCommitFailedException(_))
      .getOrElse(new IllegalStateException("Unexpected error without cause exception.")))
  }

  private def createFetchRequest(offsetsToVerify: OffsetMap): OffsetFetchRequest = {
    new OffsetFetchRequest(
      kafkaConsumer.props.groupId,
      offsetsToVerify.toFetchRequestInfo,
      versionId = OffsetCommitRequest.CurrentVersion,
      correlationId = correlationId
    )
  }

  private def handleFetchErrorCode(
    offsetsToVerify: OffsetMap,
    retriesLeft: Int,
    ex: KafkaErrorException
  ): Try[OffsetMap] = {
    if (offsetManagerHasMoved(ex.statuses.values))
      findNewCoordinatorAndRetry(offsetsToVerify, retriesLeft, getOffsetsTrial)
    else if (offsetLoadInProgress(ex.statuses.values))
      retry(offsetsToVerify, retriesLeft, OffssetLoadInProgressException, getOffsetsTrial)
    else
      retry(offsetsToVerify, retriesLeft, ex, getOffsetsTrial)
  }

  private def offsetLoadInProgress(errors: Iterable[Short]): Boolean = {
    errors.exists(_ == OffsetsLoadInProgressCode)
  }

  private def offsetManagerHasMoved(errors: Iterable[Short]): Boolean = {
    errors.exists(
      err => err == NotCoordinatorForConsumerCode || err == ConsumerCoordinatorNotAvailableCode
    )
  }

  private def hasError(offsetFetchResponse: OffsetFetchResponse) = {
    offsetFetchResponse.requestInfo.exists{ case (topicAndPartition, meta) => meta.error != ErrorMapping.NoError }
  }

  def findNewCoordinator[T](
    onFound: () => Try[T],
    lastErr: Option[Throwable] = None,
    retriesLeft: Int = NativeCommitter.MaxRetries
  ): Try[T] = {
    if (retriesLeft == 0)
      Failure(new IllegalStateException(
        "Cannot connect to coordinator",
        lastErr.getOrElse(new IllegalStateException("Unknown reason"))
      ))
    else {
      channel.disconnect()
      val resolveResult = offsetManagerResolver.resolve(kafkaConsumer)
      resolveResult match {
        case Failure(exception) => findNewCoordinator(onFound, Some(exception), retriesLeft - 1)
        case Success(newChannel) =>
          this.channel = newChannel
          onFound()
      }
    }
  }

  private def findNewCoordinatorAndRetry[T](
    offsetsToVerify: OffsetMap,
    retriesLeft: Int,
    funToRetry: (OffsetMap, Int, Option[Throwable]) => Try[T]
  ): Try[T] = {
    def afterFindingNewCoordinator(): Try[T] = retry(offsetsToVerify, retriesLeft, noCoordinatorException, funToRetry)
    findNewCoordinator(afterFindingNewCoordinator)
  }

  private def noCoordinatorException = new CoordinatorNotFoundException(kafkaConsumer.props.brokerList)

  private def retry[T](
    offsets: OffsetMap,
    retriesLeft: Int,
    cause: Throwable,
    funToRetry: (OffsetMap, Int, Option[Throwable]) => Try[T]
  ): Try[T] = {
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
  val RetryIntervalMs: Long = 400
}

private[native] object KafkaSendCommit extends ((BlockingChannel, OffsetCommitRequest) => Try[OffsetCommitResponse]) {
  override def apply(channel: BlockingChannel, req: OffsetCommitRequest) = {
    Try {
      channel.send(req)
      OffsetCommitResponse.readFrom(channel.receive().buffer)
    }
  }
}

private[native] object KafkaSendFetch extends ((BlockingChannel, OffsetFetchRequest) => Try[OffsetFetchResponse]) {
  override def apply(channel: BlockingChannel, req: OffsetFetchRequest) = {
    Try {
      channel.send(req)
      OffsetFetchResponse.readFrom(channel.receive().buffer)
    }
  }
}

private[native] class NativeCommitFailedException(cause: Throwable)
  extends Exception(s"Failed to commit after ${NativeCommitter.MaxRetries} retires", cause)

private[native] object OffssetLoadInProgressException
  extends Exception("Cannot fetch offsets, coordinator responded with OffsetsLoadInProgressCode")

private[native] class NativeGetOffsetsFailedException(cause: Throwable)
  extends Exception(s"Failed to fetch offset data after ${NativeCommitter.MaxRetries} retires", cause)

private[native] class CoordinatorNotFoundException(brokerList: String)
  extends Exception(s"Could not find offset manager in broker list: $brokerList")

private[native] class KafkaErrorException(val statuses: Map[TopicAndPartition, Short])
  extends Exception(s"Received statuses: $statuses")