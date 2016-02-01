package com.softwaremill.react.kafka.commit

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.{ConsumerProperties, KafkaTest}
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.consumer.KafkaConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.mockito.BDDMockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Try, Success}

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

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  it should "not call flush until a message arrives" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    implicit val offsetCommitter = new AlwaysSuccessfullTestCommitter()
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    startCommitterActor(committerFactory, consumer)

    // then
    awaitCond {
      offsetCommitter.started
    }
    verifyNever(offsetCommitter.totalFlushCount > 0)
  }

  it should "commit offset 0" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    implicit val offsetCommitter = new AlwaysSuccessfullTestCommitter()
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    val actor = startCommitterActor(committerFactory, consumer)
    actor ! msg(partition = 0, offset = 0L)

    // then
    ensureLastCommitted(partition = 0, offset = 0L)
  }

  it should "not commit smaller offset" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    implicit val offsetCommitter = new AlwaysSuccessfullTestCommitter()
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    val actor = startCommitterActor(committerFactory, consumer)
    actor ! msg(partition = 0, offset = 5L)
    ensureLastCommitted(partition = 0, offset = 5L)
    actor ! msg(partition = 0, offset = 3L)

    // then
    verifyNever(offsetCommitter.lastCommittedOffsetFor(partition = 0).equals(Some(3L)))
  }

  it should "commit larger offset" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    implicit val offsetCommitter = new AlwaysSuccessfullTestCommitter()
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    val actor = startCommitterActor(committerFactory, consumer)
    actor ! msg(partition = 0, offset = 5L)
    actor ! msg(partition = 1, offset = 151L)
    actor ! msg(partition = 0, offset = 152L)
    actor ! msg(partition = 1, offset = 190L)

    // then
    ensureLastCommitted(partition = 0, offset = 152L)
    ensureLastCommitted(partition = 1, offset = 190L)
  }

  it should "not commit the same offset twice" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    implicit val offsetCommitter = new AlwaysSuccessfullTestCommitter()
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    val actor = startCommitterActor(committerFactory, consumer)
    actor ! msg(partition = 0, offset = 5L)
    actor ! msg(partition = 1, offset = 151L)
    actor ! msg(partition = 1, offset = 190L)
    ensureLastCommitted(0, 5L)
    ensureLastCommitted(1, 190L)

    // then
    ensureExactlyOneFlush(0, 5L)
    ensureExactlyOneFlush(1, 190L)
  }

  it should "try to re-establish channel" in {
    // given
    val consumer = givenConsumer(commitInterval = 500 millis)
    implicit val offsetCommitter = new OccasionallyFailingCommitter(failingReqNo = 1)
    val committerFactory = givenOffsetCommitter(consumer, offsetCommitter)

    // when
    val actor = startCommitterActor(committerFactory, consumer)
    actor ! msg(partition = 0, offset = 5L)
    actor ! msg(partition = 1, offset = 151L)
    actor ! msg(partition = 1, offset = 190L)
    ensureLastCommitted(0, 5L)
    ensureLastCommitted(1, 190L)

    // then
    offsetCommitter.restartCount should be(1)
  }

  def ensureExactlyOneFlush(partition: Int, offset: Long)(implicit offsetCommitter: AlwaysSuccessfullTestCommitter): Unit = {
    verifyNever(offsetCommitter.flushCount((partition, offset)) != 1)
  }

  def startCommitterActor(committerFactory: CommitterProvider, consumer: KafkaConsumer[String]) = {
    system.actorOf(Props(new ConsumerCommitter(committerFactory, consumer)))
  }

  def ensureLastCommitted(partition: Int, offset: Long)(implicit offsetCommitter: CommiterVerifications): Unit = {
    awaitCond {
      offsetCommitter.lastCommittedOffsetFor(partition).equals(Some(offset))
    }
  }

  def msg(partition: Int, offset: Long) =
    MessageAndMetadata(topic, partition, null, offset, keyDecoder, valueDecoder)

  def givenConsumer(commitInterval: FiniteDuration) = {
    val consumer = mock[KafkaConsumer[String]]
    val properties = ConsumerProperties(kafkaHost, zkHost, topic, "groupId", valueDecoder)
    given(consumer.commitInterval).willReturn(commitInterval)
    given(consumer.props).willReturn(properties)
    consumer
  }

  def givenOffsetCommitter(consumer: KafkaConsumer[String], committer: OffsetCommitter) = {
    val factory = mock[CommitterProvider]
    given(factory.create(consumer)).willReturn(Success(committer))
    factory
  }

}

trait CommiterVerifications {
  def lastCommittedOffsetFor(partition: Int): Option[Long]
}

class OccasionallyFailingCommitter(failingReqNo: Int) extends OffsetCommitter with CommiterVerifications {
  val innerSuccesfullCommitter = new AlwaysSuccessfullTestCommitter
  var consumedRequests = 0
  var restartCount = 0

  override def commit(offsets: OffsetMap): Try[OffsetMap] = {
    consumedRequests = consumedRequests + 1
    if (consumedRequests == failingReqNo)
      Failure(new RuntimeException("Could not commit"))
    else
      innerSuccesfullCommitter.commit(offsets)
  }

  override def tryRestart(): Try[Unit] = {
    restartCount = restartCount + 1
    Success(())
  }

  override def lastCommittedOffsetFor(partition: Int) =
    innerSuccesfullCommitter.lastCommittedOffsetFor(partition)
}

class AlwaysSuccessfullTestCommitter extends OffsetCommitter with CommiterVerifications {
  var started, stopped: Boolean = false
  var innerMap: Offsets = Map.empty
  var flushCount: Map[(Int, Long), Int] = Map.empty

  override def commit(offsets: OffsetMap): Try[OffsetMap] = {
    innerMap = offsets.map
    innerMap.foreach {
      case (TopicAndPartition(topic, partition), offset) =>
        val currentFlushCount = flushCount.getOrElse((partition, offset), 0)
        flushCount = flushCount + ((partition, offset) -> (currentFlushCount + 1))
    }
    Success(OffsetMap(innerMap))
  }

  def lastCommittedOffsetFor(partition: Int) = innerMap.find {
    case (TopicAndPartition(_, p), _) => p == partition
  }.map {
    case (TopicAndPartition(_, p), o) => o
  }

  def totalFlushCount = flushCount.values.sum

  override def start(): Unit = started = true

  override def stop(): Unit = stopped = true

  override def tryRestart(): Try[Unit] = Success(())
}