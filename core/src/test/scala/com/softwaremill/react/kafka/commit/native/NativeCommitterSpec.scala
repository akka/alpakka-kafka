package com.softwaremill.react.kafka.commit.native

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.softwaremill.react.kafka.KafkaTest
import com.softwaremill.react.kafka.commit.OffsetMap
import kafka.api._
import kafka.common.{ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel
import org.mockito.BDDMockito._
import org.mockito.Matchers.{any, eq => meq}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, Matchers, fixture}

import scala.language.existentials
import scala.util.{Failure, Success, Try}

class NativeCommitterSpec extends TestKit(ActorSystem("NativeCommitterSpec"))
    with fixture.FlatSpecLike with Matchers with KafkaTest with MockitoSugar with BeforeAndAfterEach {

  behavior of "Native committer"

  val offsetMap = OffsetMap(Map(TopicAndPartition("topic", 0) -> 1L))

  case class CommitterFixture(
    consumer: KafkaConsumer[_],
    offsetManagerResolver: OffsetManagerResolver,
    initialChannel: BlockingChannel,
    var sendCommit: (BlockingChannel, OffsetCommitRequest) => Try[OffsetCommitResponse] = KafkaSendCommit,
    var sendFetch: (BlockingChannel, OffsetFetchRequest) => Try[OffsetFetchResponse] = KafkaSendFetch
  )

  type FixtureParam = CommitterFixture

  def withFixture(test: OneArgTest) = {
    val kafka = newKafka()
    val properties = consumerProperties(FixtureParam("topic", "groupId", kafka))
    val consumer = new KafkaConsumer(properties)
    val managerResolver = mock[OffsetManagerResolver]
    val initialChannel = mock[BlockingChannel]
    val theFixture = CommitterFixture(consumer, managerResolver, initialChannel)
    withFixture(test.toNoArgTest(theFixture))
  }

  private def newCommitter(
    kafkaConsumer: KafkaConsumer[_],
    offsetManagerResolver: OffsetManagerResolver,
    channel: BlockingChannel,
    sendCommit: (BlockingChannel, OffsetCommitRequest) => Try[OffsetCommitResponse],
    sendFetch: (BlockingChannel, OffsetFetchRequest) => Try[OffsetFetchResponse]
  ): NativeCommitter = {
    new NativeCommitter(kafkaConsumer, offsetManagerResolver, channel, sendCommit, sendFetch)
  }

  private def newCommitter(f: CommitterFixture): NativeCommitter =
    newCommitter(f.consumer, f.offsetManagerResolver, f.initialChannel, f.sendCommit, f.sendFetch)

  it should "Fail on exception when setting offsets" in { f =>
    // given
    val exception = new IllegalStateException("fatal!")
    given(f.initialChannel.send(any[RequestOrResponse])).willThrow(exception)
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(true)
    result.failed.get.getCause should equal(exception)
  }

  it should "Fail when trying to switch to new manager on commit" in { implicit f =>
    // given
    val channelResolvingException = new IllegalStateException("fatal!")
    givenResponseWithErrorCode(ErrorMapping.NotCoordinatorForConsumerCode)
    given(f.offsetManagerResolver.resolve(meq(f.consumer), any[Int], any[Option[Throwable]]))
      .willReturn(Failure(channelResolvingException))
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(true)
    result.failed.get.getCause should equal(channelResolvingException)
  }

  it should "Eventually succeed when a new manager gets found" in { implicit f =>
    // given
    val channelResolvingException = new IllegalStateException("fatal!")
    givenResponseWithErrorCode(ErrorMapping.NotCoordinatorForConsumerCode, howManyTimes = 1)
    given(f.offsetManagerResolver.resolve(meq(f.consumer), any[Int], any[Option[Throwable]]))
      .willReturn(Failure(channelResolvingException))
    givenFetchSuccess()
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(false)
  }

  it should "Eventually succeed when a failing commit finally passes" in { implicit f =>
    // given
    givenResponseWithErrorCode(ErrorMapping.MessageSizeTooLargeCode, howManyTimes = 3)
    givenFetchSuccess()
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(false)
  }

  it should "Fail when a commit fails with too many retries" in { implicit f =>
    // given
    givenResponseWithErrorCode(ErrorMapping.MessageSizeTooLargeCode, howManyTimes = 6)
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(true)
    val expectedError = "Received statuses: Map([topic,0] -> 10)"
    val cause: Throwable = result.failed.get.getCause
    cause.getClass should equal(classOf[KafkaErrorException])
    cause.getMessage should equal(expectedError)
  }

  it should "Fail when fetch fails with too many retries" in { implicit f =>
    // given
    givenSuccessfullCommit()
    givenFetchResponse(ErrorMapping.InvalidTopicCode, howManyTimes = 10)
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(true)
    val expectedError = "Received statuses: Map([topic,0] -> 17)"
    val cause: Throwable = result.failed.get.getCause
    cause.getClass should equal(classOf[KafkaErrorException])
    cause.getMessage should equal(expectedError)
  }

  it should "Fail when fetch fails on resolving new coordinator" in { implicit f =>
    // given
    givenSuccessfullCommit()
    val expectedError = new IllegalStateException("Cannot the coordinator")
    givenFetchResponse(ErrorMapping.NotCoordinatorForConsumerCode, howManyTimes = 10)
    given(f.offsetManagerResolver.resolve(meq(f.consumer), any[Int], any[Option[Throwable]]))
      .willReturn(Failure(expectedError))
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(true)
    result.failed.get.getCause should equal(expectedError)
  }

  it should "Fail when offset load is continously in progress" in { implicit f =>
    // given
    givenSuccessfullCommit()
    givenFetchResponse(ErrorMapping.OffsetsLoadInProgressCode, howManyTimes = 10)
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(true)
    result.failed.get.getCause should equal(OffssetLoadInProgressException)
  }
  it should "Succeed when offset load in progress a few times but then fine" in { implicit f =>
    // given
    givenSuccessfullCommit()
    givenFetchResponse(ErrorMapping.OffsetsLoadInProgressCode, howManyTimes = 3)
    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(false)
  }

  it should "Succeed when finds new offset manager" in { implicit f =>
    // given
    givenSuccessfullCommit()
    val newChannel = mock[BlockingChannel]
    given(f.offsetManagerResolver.resolve(meq(f.consumer), any[Int], any[Option[Throwable]]))
      .willReturn(Success(newChannel))
    givenFetchResponse(ErrorMapping.NotCoordinatorForConsumerCode, howManyTimes = 1, okForChannel = newChannel)

    val committer = newCommitter(f)

    // when
    val result: Try[OffsetMap] = committer.commit(OffsetMap())

    // then
    result.isFailure should equal(false)
  }

  private def givenSuccessfullCommit()(implicit f: FixtureParam): Unit =
    givenResponseWithErrorCode(ErrorMapping.NoError, 1)

  private def givenResponseWithErrorCode(code: Short, howManyTimes: Int = 10)(implicit f: FixtureParam): Unit = {
    var retries = 0
    f.sendCommit = (channel, req) => {
      val finalCode = if (retries <= howManyTimes)
        code
      else
        ErrorMapping.NoError

      retries = retries + 1
      Success(OffsetCommitResponse(Map(TopicAndPartition("topic", 0) -> finalCode)))
    }
  }

  private def givenFetchSuccess()(implicit f: FixtureParam) =
    givenFetchResponse(ErrorMapping.NoError, 0)

  private def givenFetchResponse(code: Short, howManyTimes: Int = 10,
    okForChannel: BlockingChannel = mock[BlockingChannel])(implicit f: FixtureParam): Unit = {
    def resp(code: Short) =
      Success(OffsetFetchResponse(Map(TopicAndPartition("topic", 0) -> OffsetMetadataAndError(0, error = code))))

    var retries = 0
    f.sendFetch = (channel, req) => {

      val finalCode = if (retries <= howManyTimes && channel != okForChannel)
        code
      else
        ErrorMapping.NoError

      retries = retries + 1
      resp(finalCode)
    }
  }
}
