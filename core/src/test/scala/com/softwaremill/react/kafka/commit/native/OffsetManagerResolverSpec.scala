package com.softwaremill.react.kafka.commit.native

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.softwaremill.react.kafka.{ConsumerProperties, KafkaTest}
import kafka.api.{ConsumerMetadataRequest, ConsumerMetadataResponse}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel
import org.mockito.BDDMockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, Matchers, fixture}

import scala.util.Try

class OffsetManagerResolverSpec extends TestKit(ActorSystem("OffsetManagerResolverSpec"))
    with fixture.FlatSpecLike with Matchers with KafkaTest with MockitoSugar with BeforeAndAfterEach {

  var channelMock: BlockingChannel = _
  var channelMockFactory: (String, Int) => BlockingChannel = _

  case class OffsetManagerFixture() {
    var consumer: KafkaConsumer[String] = _
    def createConsumer(consumerProperties: ConsumerProperties[String]) = {
      consumer = new KafkaConsumer(consumerProperties)
      consumer
    }

    def clearResources(): Unit = consumer.close()
  }

  type FixtureParam = OffsetManagerFixture

  def withFixture(test: OneArgTest) = {
    val theFixture = OffsetManagerFixture()
    try withFixture(test.toNoArgTest(theFixture))
    finally {
      theFixture.clearResources()
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    channelMock = mock[BlockingChannel]
    channelMockFactory = (host: String, port: Int) => {
      given(channelMock.host).willReturn(host)
      given(channelMock.port).willReturn(port)
      channelMock
    }
  }

  behavior of "Offset manager resolver"

  it should "use broker if it equals coordinator" in { f =>
    // given
    val properties = consumerProperties(FixtureParam("topic", "groupId", kafka))
    val consumer = f.createConsumer(properties)
    val metadata = givenCoordinatorMetadata(broker(kafkaHost), ErrorMapping.NoError)
    val reader = givenCoordinatorRequestWillReturn(metadata)

    // when
    val result: Try[BlockingChannel] = new OffsetManagerResolver(channelMockFactory, reader).resolve(consumer)

    // then
    result.isSuccess should be(true)
    result.get.host should equal("localhost")
    result.get.port should equal(9092)
  }

  it should "use coordinator if it's different than initial channel" in { f =>
    // given
    val properties = consumerProperties(FixtureParam("topic", "groupId", kafka))
    val consumer = f.createConsumer(properties)
    val metadata = givenCoordinatorMetadata(broker("otherhost:12535"), ErrorMapping.NoError)
    val reader = givenCoordinatorRequestWillReturn(metadata)

    // when
    val result: Try[BlockingChannel] = new OffsetManagerResolver(channelMockFactory, reader).resolve(consumer)

    // then
    result.isSuccess should be(true)
    result.get.host should equal("otherhost")
    result.get.port should equal(12535)
  }

  it should "handle error when connecting to initial channel" in { f =>
    // given
    val properties = consumerProperties(FixtureParam("topic", "groupId", kafka))
    val consumer = f.createConsumer(properties)
    given(channelMock.connect()).willThrow(new NullPointerException("Channel closed"))

    // when
    val result: Try[BlockingChannel] = new OffsetManagerResolver(channelMockFactory).resolve(consumer)

    // then
    result.isFailure should be(true)
    result.failed.get shouldBe a[OffsetManagerResolvingException]
    result.failed.get.getCause shouldBe a[NullPointerException]
  }

  it should "handle error when connecting to coordinator" in { f =>
    // given
    val properties = consumerProperties(FixtureParam("topic", "groupId", kafka))
    val consumer = f.createConsumer(properties)
    val metadata = givenCoordinatorMetadata(broker("otherhost:12535"), ErrorMapping.NoError)
    val reader = givenCoordinatorRequestWillReturn(metadata)
    val coordinatorMock = mock[BlockingChannel]

    channelMockFactory = (host: String, port: Int) => {
      val mock = if (host == "localhost") channelMock else coordinatorMock
      given(mock.host).willReturn(host)
      given(mock.port).willReturn(port)
      mock
    }
    given(coordinatorMock.connect()).willThrow(new IllegalArgumentException("Channel closed"))

    // when
    val result: Try[BlockingChannel] = new OffsetManagerResolver(channelMockFactory, reader).resolve(consumer)
    consumer.close()

    // then
    result.isFailure should be(true)
    result.failed.get shouldBe a[OffsetManagerResolvingException]
    result.failed.get.getCause shouldBe a[IllegalArgumentException]
  }

  it should "handle error when resolving the coordinator" in { f =>
    // given
    val properties = consumerProperties(FixtureParam("topic", "groupId", kafka))
    val consumer = f.createConsumer(properties)
    val metadata = givenCoordinatorMetadata(broker("otherhost:12535"), ErrorMapping.LeaderNotAvailableCode)
    val reader = givenCoordinatorRequestWillReturn(metadata)

    // when
    val result: Try[BlockingChannel] = new OffsetManagerResolver(channelMockFactory, reader).resolve(consumer)

    // then
    result.isFailure should be(true)
    result.failed.get shouldBe a[OffsetManagerResolvingException]
    result.failed.get.getCause should have message "Cannot connect to coordinator. Error code: 5"
  }

  def broker(hostAndPort: String) = {
    val host = hostAndPort.split(":")(0)
    val port = hostAndPort.split(":")(1).toInt
    Some(new Broker(1, host, port))
  }

  def givenCoordinatorMetadata(coordinator: Option[Broker], errorCode: Short) =
    new ConsumerMetadataResponse(coordinator, errorCode)

  def givenCoordinatorRequestWillReturn(metadata: ConsumerMetadataResponse) = {
    (channel: BlockingChannel, request: ConsumerMetadataRequest) => metadata
  }
}
