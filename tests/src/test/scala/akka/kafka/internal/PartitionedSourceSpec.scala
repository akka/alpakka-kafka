/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util
import java.util.concurrent.atomic.AtomicReference

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage._
import akka.kafka.internal.PartitionedSourceSpec.getClass
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers, OptionValues}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

object PartitionedSourceSpec {
  def log: Logger = LoggerFactory.getLogger(getClass)

  type K = String
  type V = String
  type Record = ConsumerRecord[K, V]

  val topic = "topic"
  val singleRecord: java.util.List[Record] = Seq(new ConsumerRecord(topic, 0, 10L, "key", "value")).asJava
  val tp0 = new TopicPartition(topic, 0)
  val tp1 = new TopicPartition(topic, 1)

  def sleep(time: FiniteDuration): Unit = {
    log.debug(s"sleeping $time")
    Thread.sleep(time.toMillis)
  }
}

class PartitionedSourceSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with OptionValues
    with ScalaFutures
    with Eventually {

  implicit val patience = PatienceConfig(4.seconds, 50.millis)

  import PartitionedSourceSpec._

  def this() = this(ActorSystem())

  override def afterAll(): Unit =
    shutdown(system)

  implicit val m = ActorMaterializer(ActorMaterializerSettings(_system).withFuzzing(true))
  implicit val ec = _system.dispatcher

  def testSettings(mock: Consumer[K, V], groupId: String = "group1") =
    new ConsumerSettings(
      Map(ConsumerConfig.GROUP_ID_CONFIG -> groupId),
      Some(new StringDeserializer),
      Some(new StringDeserializer),
      pollInterval = 200.millis,
      pollTimeout = 10.millis,
      stopTimeout = 1.second,
      closeTimeout = 500.millis,
      commitTimeout = 1.second,
      wakeupTimeout = 5.seconds,
      maxWakeups = 3,
      commitRefreshInterval = Duration.Inf,
      dispatcher = "akka.kafka.default-dispatcher",
      commitTimeWarning = 1.second,
      wakeupDebug = true,
      waitClosePartition = 100.millis
    ) {
      override def createKafkaConsumer(): Consumer[K, V] =
        mock
    }

  def createCommittablePartitionedSource(
      mock: Consumer[K, V],
      groupId: String = "group1",
      topics: Set[String] = Set(topic)
  ): Source[(TopicPartition, Source[CommittableMessage[K, V], NotUsed]), Control] =
    Consumer.committablePartitionedSource(testSettings(mock, groupId), Subscriptions.topics(topics))

  class Dummy(val name: String = s"dummy_${ConsumerDummy.instanceCounter.incrementAndGet()}") extends ConsumerDummy[K, V] {
    import ConsumerDummy._

    var tps: Map[TopicPartition, TpState] = Map.empty
    var callbacks: ConsumerRebalanceListener = null
    val emptyPollData = Map.empty[TopicPartition, java.util.List[ConsumerRecord[K, V]]]
    val nextPollData: AtomicReference[Map[TopicPartition, java.util.List[ConsumerRecord[K, V]]]] =
      new AtomicReference(emptyPollData)

    def tpsRevoked: Set[TopicPartition] = tps.filter(_._2 == Revoked).keys.toSet
    def tpsAssigned: Set[TopicPartition] = tps.filter(_._2 == Assigned).keys.toSet
    def tpsResumed: Set[TopicPartition] = tps.filter(_._2 == Resumed).keys.toSet
    def tpsPaused: Set[TopicPartition] = tps.filter(_._2 == Paused).keys.toSet

    override def assignment(): java.util.Set[TopicPartition] = (tpsAssigned ++ tpsResumed ++ tpsPaused).asJava
    override def subscribe(topics: java.util.Collection[String], callback: ConsumerRebalanceListener): Unit =
      callbacks = callback
    override def assign(partitions: java.util.Collection[TopicPartition]): Unit = {
      callbacks.onPartitionsRevoked(tps.keySet.asJavaCollection)
      tps = partitions.asScala.map(_ -> Assigned).toMap
      callbacks.onPartitionsAssigned(partitions)
    }
    override def poll(timeout: Long): ConsumerRecords[K, V] = {
      val data = nextPollData.get()
      val (data2, dataPaused) = data.partition {
        case (tp, _) => tpsResumed.contains(tp)
      }
      nextPollData.set(dataPaused)
      if (data2.nonEmpty) {
        log.debug(s"poll result $data2")
      }
      new ConsumerRecords[K, V](data2.asJava)
    }
    override def position(partition: TopicPartition): Long = 0
    override def paused(): java.util.Set[TopicPartition] = tpsPaused.asJava
    override def pause(partitions: java.util.Collection[TopicPartition]): Unit = {
      super.pause(partitions)
      val ps = partitions.asScala
      log.debug(s"pausing ${ps.mkString("(", ", ", ")")}")
      tps = tps ++ ps.map(_ -> Paused)
    }
    override def resume(partitions: java.util.Collection[TopicPartition]): Unit = {
      val ps = partitions.asScala
      log.debug(s"resuming ${ps.mkString("(", ", ", ")")}")
      tps = tps ++ ps.map(_ -> Resumed)
    }
  }

  "partitioned source" should "resume topics with demand" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sinkQueue = createCommittablePartitionedSource(dummy)
      .flatMapMerge(breadth = 10, _._2)
      .runWith(Sink.queue())

    dummy.started.futureValue should be (Done)

    dummy.tpsPaused should be('empty)
    dummy.assign(Set(tp0, tp1).asJavaCollection)
    dummy.nextPollData.set(Map(tp0 -> singleRecord))
    sinkQueue.pull().futureValue.value.record.value() should be("value")
    dummy.nextPollData.set(Map(tp1 -> singleRecord))
    sinkQueue.pull().futureValue.value.record.value() should be("value")

    dummy.tpsResumed should contain allOf (tp0, tp1)
    dummy.tpsPaused should be('empty)
    dummy.assign(Set(tp0).asJavaCollection)

    eventually {
      dummy.tpsResumed should contain only tp0
    }

    dummy.tpsPaused should be('empty)

    sinkQueue.cancel()
  }

  it should "cancel the sub-source when partition is revoked" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sinkQueue = createCommittablePartitionedSource(dummy)
      .runWith(Sink.queue())

    dummy.started.futureValue should be (Done)

    dummy.tpsPaused should be('empty)
    dummy.assign(Set(tp0, tp1).asJavaCollection)

    // Two (TopicParition, Source) tuples should be issued
    val tup0 = sinkQueue.pull().futureValue.value
    val tup1 = sinkQueue.pull().futureValue.value
    val subSources = Map(tup0, tup1)
    subSources.keys should contain allOf (tp0, tp1)
    // No demand on sub-sources => paused
    dummy.tpsPaused should contain allOf (tp0, tp1)

    dummy.assign(Set(tp0).asJavaCollection)

    // tp1 not assigned any more => corresponding source should be cancelled
    subSources(tp1).runWith(Sink.ignore).futureValue should be(Done)

    sinkQueue.cancel()
  }

  it should "pause when there is no demand" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sinkQueue = createCommittablePartitionedSource(dummy)
      .runWith(Sink.queue())

    dummy.started.futureValue should be (Done)

    dummy.tpsPaused should be('empty)
    dummy.assign(Set(tp0, tp1).asJavaCollection)

    // (TopicParition, Source) tuples should be issued
    val tup0 = sinkQueue.pull().futureValue.value
    val tup1 = sinkQueue.pull().futureValue.value
    val subSources = Map(tup0, tup1)
    subSources.keys should contain allOf (tp0, tp1)
    // No demand on sub-sources => paused
    dummy.tpsPaused should contain allOf (tp0, tp1)

    dummy.assign(Set(tp0).asJavaCollection)

    // No demand on sub-sources => paused
    dummy.tpsPaused should contain only tp0

    val probeTp0 = subSources(tp0).runWith(TestSink.probe[CommittableMessage[K, V]])
    dummy.nextPollData.set(Map(tp0 -> singleRecord))

    // demand a value
    probeTp0.requestNext().record.value() should be("value")
    // no demand anymore should lead to paused partition
    eventually {
      dummy.tpsPaused should contain(tp0)
    }
    probeTp0.cancel()
    sinkQueue.cancel()
  }
}
