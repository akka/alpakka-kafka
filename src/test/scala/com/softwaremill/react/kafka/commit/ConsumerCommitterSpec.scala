package com.softwaremill.react.kafka.commit

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.{ConsumerProperties, KafkaTest}
import com.typesafe.config.ConfigFactory
import kafka.consumer.KafkaConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.mockito.BDDMockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._
import scala.language.postfixOps

class ConsumerCommitterSpec extends TestKit(ActorSystem(
  "ConsumerCommitterSpec",
  ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")
)) with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
    with KafkaTest with MockitoSugar {

  implicit val timeout = Timeout(1 second)

  behavior of "Consumer committer"
  val topic = "topicName"
  val valueDecoder: StringDecoder = new StringDecoder()
  val keyDecoder = valueDecoder

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  it should "call flush after given commitInterval" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    val offsetCommitter = new AlwaysSuccessfullTestCommitter()
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    startCommitterActor(committerFactory, consumer)

    // then
    awaitCond {
      offsetCommitter.started &&
        offsetCommitter.flushCount > 1
    }
  }

  it should "commit offset 0" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    val offsetCommitter = new AlwaysSuccessfullTestCommitter()
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    val actor = startCommitterActor(committerFactory, consumer)
    actor ! msg(partition = 0, offset = 0L)

    // then
    awaitCond {
      offsetCommitter.lastCommittedOffsetFor(partition = 0).contains(0L)
    }
  }

  def startCommitterActor(committerFactory: CommitterFactory, consumer: KafkaConsumer[String]) = {
    system.actorOf(Props(new ConsumerCommitter(committerFactory, consumer)))
  }

  def msg(partition: Int, offset: Long) =
    MessageAndMetadata(topic, partition = 0, null, 0L, keyDecoder, valueDecoder)

  def givenConsumer(commitInterval: FiniteDuration) = {
    val consumer = mock[KafkaConsumer[String]]
    val properties = ConsumerProperties(kafkaHost, zkHost, topic, "groupId", valueDecoder)
    given(consumer.commitInterval).willReturn(commitInterval)
    given(consumer.props).willReturn(properties)
    consumer
  }

  def givenOffsetCommitter(consumer: KafkaConsumer[String], committer: OffsetCommitter) = {
    val factory = mock[CommitterFactory]
    given(factory.create(consumer)).willReturn(Right(committer))
    factory
  }
}

class AlwaysSuccessfullTestCommitter extends OffsetCommitter {
  var started, stopped: Boolean = false
  var innerMap: OffsetMap = Map.empty
  var flushCount = 0

  override def commit(offsets: OffsetMap): OffsetMap = {
    innerMap = offsets
    flushCount = flushCount + 1
    innerMap
  }

  def lastCommittedOffsetFor(partition: Int) = innerMap.find {
    case ((_, p), _) => p == partition
  }.map {
    case ((_, p), o) => 0
  }

  override def start(): Unit = started = true

  override def stop(): Unit = stopped = true
}