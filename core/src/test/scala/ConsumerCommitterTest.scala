package test

import akka.actor._
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka._
import com.softwaremill.react.kafka.commit.{Offsets, OffsetMap, ConsumerCommitter}
import com.softwaremill.react.kafka.KafkaActorPublisher.{CommitAck, CommitOffsets}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import test.tools.KafkaTest

class ConsumerCommitterTest extends TestKit(ActorSystem(
  "ConsumerCommitterSpec",
  ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")
)) with ImplicitSender with fixture.FlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
    with KafkaTest with MockitoSugar {

  implicit val timeout = Timeout(1 second)

  behavior of "Consumer committer"
  val topic = "topicName"
  val strDeserializer = new StringDeserializer()

  case class CommitterFixtureParam(consumer: ActorRef, failingConsumer: ActorRef)
  type FixtureParam = CommitterFixtureParam

  def withFixture(test: OneArgTest) = {
    val committer = system.actorOf(Props(new AlwaysSuccessfullTestCommitter))
    val failingCommitter = system.actorOf(Props(new OccasionallyFailingCommitter(failingReqNo = 2, committer)))
    val theFixture = CommitterFixtureParam(committer, failingCommitter)
    try {
      withFixture(test.toNoArgTest(theFixture))
    }
    finally {
      system.stop(committer)
      system.stop(failingCommitter)
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  it should "not call flush until a message arrives" in { implicit f =>
    // when
    startCommitterActor(f.consumer, commitInterval = 100 milliseconds)

    // then
    verifyNever(committerFlushSum > 0)
  }

  it should "commit offset 0" in { implicit f =>
    // when
    val committer = startCommitterActor(f.consumer, commitInterval = 100 milliseconds)
    committer ! msg(partition = 0, offset = 0L)

    // then
    verifyLastCommitted(partition = 0, offset = 0L)
  }

  it should "not commit smaller offset" in { implicit f =>
    // when
    val committer = startCommitterActor(f.consumer, commitInterval = 100 milliseconds)

    committer ! msg(partition = 0, offset = 5L)
    verifyLastCommitted(partition = 0, offset = 5L)
    committer ! msg(partition = 0, offset = 3L)

    // then
    verifyNever(lastCommitted(partition = 0).contains(3L))
  }

  it should "commit larger offset" in { implicit f =>
    // when
    val committer = startCommitterActor(f.consumer, commitInterval = 500 milliseconds)
    committer ! msg(partition = 0, offset = 5L)
    committer ! msg(partition = 1, offset = 151L)
    committer ! msg(partition = 0, offset = 152L)
    committer ! msg(partition = 1, offset = 190L)

    // then
    verifyLastCommitted(partition = 0, offset = 152L)
    verifyLastCommitted(partition = 1, offset = 190L)
  }

  it should "not commit the same offset twice" in { implicit f =>
    // when
    val committer = startCommitterActor(f.consumer, commitInterval = 500 milliseconds)
    committer ! msg(partition = 0, offset = 5L)
    committer ! msg(partition = 1, offset = 151L)
    committer ! msg(partition = 1, offset = 190L)
    verifyLastCommitted(0, 5L)
    verifyLastCommitted(1, 190L)

    // then
    ensureExactlyOneFlush(0, 5L)
    ensureExactlyOneFlush(1, 190L)
  }

  it should "die when the committer dies" in { implicit f =>
    // when
    watch(f.failingConsumer)
    val committer = startCommitterActor(f.failingConsumer, commitInterval = 500 milliseconds)
    watch(committer)
    committer ! msg(partition = 0, offset = 5L)
    verifyLastCommitted(0, 5L)
    committer ! msg(partition = 1, offset = 151L)

    // then
    expectTerminated(f.failingConsumer)
    expectTerminated(committer)
  }

  def committerFlushSum(implicit f: FixtureParam): Int =
    Await.result(f.consumer ? GetTotalFlushCount, atMost = 1 second).asInstanceOf[Int]

  def flushCount(partition: Int, offset: Long)(implicit f: FixtureParam): Option[Int] =
    Await.result(f.consumer ? GetFlushCount(partition, offset), atMost = 1 second).asInstanceOf[Option[Int]]

  def ensureExactlyOneFlush(partition: Int, offset: Long)(implicit f: FixtureParam): Unit = {
    verifyNever(!flushCount(partition, offset).contains(1))
  }

  def lastCommitted(partition: Int)(implicit f: FixtureParam): Option[Int] = {
    Await.result(f.consumer ? GetLastCommittedOffsetFor(partition), atMost = 1 second).asInstanceOf[Option[Int]]
  }

  def verifyLastCommitted(partition: Int, offset: Long)(implicit f: FixtureParam): Unit = {
    awaitCond(lastCommitted(partition).contains(offset), max = 3 seconds, interval = 200 millis)
  }

  def startCommitterActor(consumerActor: ActorRef, commitInterval: FiniteDuration) = {
    system.actorOf(Props(new ConsumerCommitter(consumerActor, consumerProperties(commitInterval))))
  }

  def msg(partition: Int, offset: Long) =
    new ConsumerRecord(topic, partition, offset, null, null)

  def consumerProperties(commitInterval: FiniteDuration) =
    ConsumerProperties(kafkaHost, topic, "groupId", strDeserializer, strDeserializer)
      .commitInterval(commitInterval)

  def alwaysSuccessfullCommitter = system.actorOf(Props(new AlwaysSuccessfullTestCommitter))
}

class OccasionallyFailingCommitter(failingReqNo: Int, innerCommitter: ActorRef) extends Actor {
  var consumedRequests = 0

  override def receive: Actor.Receive = {
    case CommitOffsets(offsets) => commit(offsets)
    case msg: GetLastCommittedOffsetFor => innerCommitter forward msg
    case GetTotalFlushCount => innerCommitter forward GetTotalFlushCount
  }

  def commit(offsets: OffsetMap) = {
    consumedRequests = consumedRequests + 1
    if (consumedRequests == failingReqNo)
      context.stop(self)
    else
      innerCommitter forward CommitOffsets(offsets)
  }
}

class AlwaysSuccessfullTestCommitter extends Actor {

  var innerMap: Offsets = Map.empty
  var flushCount: Map[(Int, Long), Int] = Map.empty

  override def receive: Actor.Receive = {
    case CommitOffsets(offsets) => commit(offsets)
    case GetLastCommittedOffsetFor(partition) => sender() ! lastCommittedOffsetFor(partition)
    case GetTotalFlushCount => sender() ! totalFlushCount
    case GetFlushCount(partition, offset) => sender() ! flushCount.get((partition, offset))
  }

  def commit(offsets: OffsetMap): Unit = {
    innerMap = offsets.map
    innerMap.foreach {
      case tp =>
        val partition = tp._1.partition()
        val offset = tp._2
        val currentFlushCount = flushCount.getOrElse((partition, offset), 0)
        flushCount = flushCount + ((partition, offset) -> (currentFlushCount + 1))
    }
    sender() ! CommitAck(offsets)
  }

  def lastCommittedOffsetFor(partition: Int): Option[Long] = innerMap.find {
    case tp => tp._1.partition() == partition
  }.map {
    case tp => tp._2
  }

  def totalFlushCount: Int = flushCount.values.sum

}

case class GetLastCommittedOffsetFor(partition: Int)
case object GetTotalFlushCount
case class GetFlushCount(partition: Int, offset: Long)