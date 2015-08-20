package com.softwaremill.react.kafka.commit.native

import java.io.IOException

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.softwaremill.react.kafka.KafkaTest
import com.softwaremill.react.kafka.commit.OffsetMap
import kafka.api.RequestOrResponse
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}
import org.scalatest.fixture
import org.mockito.BDDMockito._
import org.mockito.Matchers.any

import scala.util.Try
import scala.language.existentials

class NativeCommitterSpec extends TestKit(ActorSystem("NativeCommitterSpec"))
    with fixture.FlatSpecLike with Matchers with KafkaTest with MockitoSugar with BeforeAndAfterEach {

  behavior of "Native committer"

  case class CommitterFixture(
    consumer: KafkaConsumer[_],
    offsetManagerResolver: OffsetManagerResolver,
    initialChannel: BlockingChannel,
    committer: NativeCommitter
  )

  type FixtureParam = CommitterFixture

  def withFixture(test: OneArgTest) = {
    val kafka = newKafka()
    val properties = consumerProperties(FixtureParam("topic", "groupId", kafka))
    val consumer = new KafkaConsumer(properties)
    val managerResolver = mock[OffsetManagerResolver]
    val initialChannel = mock[BlockingChannel]
    val committer = new NativeCommitter(consumer, managerResolver, initialChannel)
    val theFixture = CommitterFixture(consumer, managerResolver, initialChannel, committer)
    withFixture(test.toNoArgTest(theFixture))
  }

  it should "Fail on exception when setting offsets" in { f =>
    // given
    val exception = new IllegalStateException("fatal!")
    given(f.initialChannel.send(any[RequestOrResponse])).willThrow(exception)

    // when
    val result: Try[OffsetMap] = f.committer.commit(OffsetMap())

    // then
    result.isFailure should equal(true)
    result.failed.get.getCause should equal(exception)
  }


}
