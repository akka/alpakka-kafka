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

  private def givenFetchResponse(code: Short, howManyTimes: Int = 10)(implicit f: FixtureParam): Unit = {
    var retries = 0
    f.sendFetch = (channel, req) => {
      val finalCode = if (retries <= howManyTimes)
        code
      else
        ErrorMapping.NoError

      retries = retries + 1
      Success(OffsetFetchResponse(Map(TopicAndPartition("topic", 0) -> OffsetMetadataAndError(0, error = finalCode))))
    }
  }
}
