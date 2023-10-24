/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.UnaryOperator

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage._
import akka.kafka.scaladsl.Consumer
import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._

class PartitionedSourceSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with OptionValues
    with ScalaFutures
    with Eventually
    with IntegrationPatience
    with LogCapturing {

  import PartitionedSourceSpec._

  def this() =
    this(
      ActorSystem("PartitionedSourceSpec",
                  ConfigFactory
                    .parseString("""akka.stream.materializer.debug.fuzzing-mode = on""")
                    .withFallback(ConfigFactory.load()))
    )

  override def afterAll(): Unit =
    shutdown(system)

  implicit val ec: ExecutionContext = _system.dispatcher

  def consumerSettings(dummy: Consumer[K, V]): ConsumerSettings[K, V] =
    ConsumerSettings
      .create(system, new StringDeserializer, new StringDeserializer)
      .withGroupId("group")
      .withStopTimeout(10.millis)
      .withConsumerFactory(_ => dummy)

  "partitioned source" should "resume topics with demand" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink = Consumer
      .committablePartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .flatMapMerge(breadth = 10, _._2)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.tpsPaused should be(Symbol("empty"))
    dummy.assignWithCallback(tp0, tp1)
    dummy.setNextPollData(tp0 -> singleRecord)
    eventually {
      sink.requestNext().record.value() should be("value")
    }
    dummy.setNextPollData(tp1 -> singleRecord)
    sink.requestNext().record.value() should be("value")

    eventually {
      dummy.tpsResumed should contain.allOf(tp0, tp1)
      dummy.tpsPaused should be(Symbol("empty"))
    }

    dummy.assignWithCallback(tp0)

    eventually {
      dummy.tpsResumed should contain only tp0
      dummy.tpsPaused should be(Symbol("empty"))
    }

    sink.cancel()
  }

  it should "cancel the sub-source when partition is revoked" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink = Consumer
      .committablePartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.tpsPaused should be(Symbol("empty"))
    dummy.assignWithCallback(tp0, tp1)

    // Two (TopicPartition, Source) tuples should be issued
    val subSources = Map(sink.requestNext(), sink.requestNext())
    subSources.keys should contain.allOf(tp0, tp1)
    // No demand on sub-sources => paused
    dummy.tpsPaused should contain.allOf(tp0, tp1)

    dummy.assignWithCallback(tp0)

    // tp1 not assigned any more => corresponding source should be cancelled
    subSources(tp1).runWith(Sink.ignore).futureValue should be(Done)

    sink.cancel()
  }

  it should "pause when there is no demand" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink = Consumer
      .committablePartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.tpsPaused should be(Symbol("empty"))
    dummy.assignWithCallback(tp0, tp1)

    // (TopicPartition, Source) tuples should be issued
    val subSources = Map(sink.requestNext(), sink.requestNext())
    subSources.keys should contain.allOf(tp0, tp1)
    // No demand on sub-sources => paused
    dummy.tpsPaused should contain.allOf(tp0, tp1)

    dummy.assignWithCallback(tp0)

    // No demand on sub-sources => paused
    dummy.tpsPaused should contain only tp0

    val probeTp0 = subSources(tp0).runWith(TestSink[CommittableMessage[K, V]]())
    dummy.setNextPollData(tp0 -> singleRecord)

    // demand a value
    probeTp0.requestNext().record.value() should be("value")
    // no demand anymore should lead to paused partition
    eventually {
      dummy.tpsPaused should contain(tp0)
    }
    probeTp0.cancel()
    sink.cancel()
  }

  it should "get partitions assigned via callbacks" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink = Consumer
      .committablePartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    // (TopicPartition, Source) tuples should be issued
    val subSources = Map(sink.requestNext(), sink.requestNext())
    subSources.keys should contain.allOf(tp0, tp1)
    // No demand on sub-sources => paused
    dummy.tpsPaused should contain.allOf(tp0, tp1)

    val probeTp0 = subSources(tp0).runWith(TestSink[CommittableMessage[K, V]]())
    dummy.setNextPollData(tp0 -> singleRecord)

    // demand a value
    probeTp0.requestNext().record.value() should be("value")
    // no demand anymore should lead to paused partition
    eventually {
      dummy.tpsPaused should contain.allOf(tp0, tp1)
    }
    probeTp0.cancel()
    sink.cancel()
  }

  it should "complete revoked partition's source" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink = Consumer
      .committablePartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    val subSources = Map(sink.requestNext(), sink.requestNext())
    subSources.keys should contain.allOf(tp0, tp1)

    // revoke tp1
    dummy.assignWithCallback(tp0)
    subSources(tp1).runWith(Sink.ignore).futureValue should be(Done)
    // revoke tp0
    dummy.assignWithCallback()
    subSources(tp0).runWith(Sink.ignore).futureValue should be(Done)

    sink.cancel()
  }

  it should "issue new sources when assigned again" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink = Consumer
      .committablePartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    // assign 1
    dummy.assignWithCallback(tp0, tp1)
    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    // revoke tp1
    dummy.assignWithCallback(tp0)
    subSources1(tp1).runWith(Sink.ignore).futureValue should be(Done)
    // revoke tp0
    dummy.assignWithCallback()
    subSources1(tp0).runWith(Sink.ignore).futureValue should be(Done)

    // assign 2
    dummy.assignWithCallback(tp0, tp1)

    // (TopicPartition, Source) tuples should be issued
    val subSources2 = Map(sink.requestNext(), sink.requestNext())
    subSources2.keys should contain.allOf(tp0, tp1)

    sink.cancel()
  }

  "plain manual offset partitioned source" should "request offsets for partitions" in assertAllStagesStopped {
    val dummy = new Dummy()

    var assertGetOffsetsOnAssign: Set[TopicPartition] => Unit = { _ =>
      ()
    }

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      assertGetOffsetsOnAssign(tps)
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    val sink = Consumer
      .plainPartitionedManualOffsetSource(consumerSettings(dummy), Subscriptions.topics(topic), getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    // assign 1
    assertGetOffsetsOnAssign = { tps =>
      tps should contain.allOf(tp0, tp1)
    }
    dummy.assignWithCallback(tp0, tp1)

    // (TopicPartition, Source) tuples should be issued
    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    sink.cancel()
  }

  it should "after revoke request offset for remaining partition" in assertAllStagesStopped {
    val dummy = new Dummy()

    var assertGetOffsetsOnAssign: Set[TopicPartition] => Unit = { _ =>
      ()
    }

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      assertGetOffsetsOnAssign(tps)
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    val sink = Consumer
      .plainPartitionedManualOffsetSource(consumerSettings(dummy), Subscriptions.topics(topic), getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    // assign 1
    assertGetOffsetsOnAssign = { tps =>
      tps should contain.allOf(tp0, tp1)
    }
    dummy.assignWithCallback(tp0, tp1)

    // (TopicPartition, Source) tuples should be issued
    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    // revoke tp1
    assertGetOffsetsOnAssign = { tps =>
      tps should contain only tp0
    }
    dummy.assignWithCallback(tp0)
    subSources1(tp1).runWith(Sink.ignore).futureValue should be(Done)

    sink.cancel()
  }

  it should "seek to given offset" in assertAllStagesStopped {
    val dummy = new Dummy()

    var assertGetOffsetsOnAssign: Set[TopicPartition] => Unit = { _ =>
      ()
    }

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      assertGetOffsetsOnAssign(tps)
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    val sink = Consumer
      .plainPartitionedManualOffsetSource(consumerSettings(dummy), Subscriptions.topics(topic), getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    // assign 1
    assertGetOffsetsOnAssign = { tps =>
      // this fails as of #570
      tps should contain.allOf(tp0, tp1)
    }
    dummy.assignWithCallback(tp0, tp1)

    eventually {
      dummy.seeks should contain.allOf(tp0 -> 300L, tp1 -> 300L)
    }

    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    sink.cancel()
  }

  it should "call onRevoke callback" in assertAllStagesStopped {
    val dummy = new Dummy()

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    var revoked = Vector[TopicPartition]()

    val sink = Consumer
      .plainPartitionedManualOffsetSource(consumerSettings(dummy),
                                          Subscriptions.topics(topic),
                                          getOffsetsOnAssign,
                                          onRevoke = { tp =>
                                            revoked = revoked ++ tp
                                          })
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    dummy.assignWithCallback(tp0)
    eventually {
      revoked should contain theSameElementsInOrderAs Seq(tp1)
    }

    dummy.assignWithCallback()
    eventually {
      revoked should contain theSameElementsInOrderAs Seq(tp1, tp0)
    }

    sink.cancel()
  }

  it should "handle slow-loading offsets" in assertAllStagesStopped {
    val dummy = new Dummy()

    val latch = new CountDownLatch(1)

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      Future {
        latch.await(10, TimeUnit.SECONDS)
        log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
        tps.map(tp => (tp, 300L)).toMap
      }
    }

    val sink = Consumer
      .plainPartitionedManualOffsetSource(consumerSettings(dummy), Subscriptions.topics(topic), getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)
    dummy.assignWithCallback(tp0)
    latch.countDown()

    val subSources1 = Map(sink.requestNext(10.seconds))
    subSources1.keys should contain theSameElementsAs Set(tp0)

    sink.cancel()
  }

  it should "handle out-of-order loading of offsets" in assertAllStagesStopped {
    val dummy = new Dummy()

    val latch = new CountDownLatch(1)

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      // This will be called twice, but we ensure that the second returned Future completes
      // before the first returned Future
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      val offsets = tps.map(tp => (tp, 300L)).toMap
      if (tps.size == 2) {
        Future {
          latch.await(10, TimeUnit.SECONDS)
          sleep(100.milliseconds)
          offsets
        }
      } else {
        Future {
          latch.countDown()
          offsets
        }
      }
    }

    val sink = Consumer
      .plainPartitionedManualOffsetSource(consumerSettings(dummy), Subscriptions.topics(topic), getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)
    dummy.assignWithCallback(tp0)

    val subSources1 = Map(sink.requestNext(10.seconds))
    subSources1.keys should contain theSameElementsAs Set(tp0)

    sink.cancel()
  }

  "committable manual offset partitioned source" should "request offsets for partitions" in assertAllStagesStopped {
    val dummy = new Dummy()

    var assertGetOffsetsOnAssign: Set[TopicPartition] => Unit = { _ =>
      ()
    }

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      assertGetOffsetsOnAssign(tps)
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    val sink = Consumer
      .committablePartitionedManualOffsetSource(consumerSettings(dummy),
                                                Subscriptions.topics(topic),
                                                getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    // assign 1
    assertGetOffsetsOnAssign = { tps =>
      tps should contain.allOf(tp0, tp1)
    }
    dummy.assignWithCallback(tp0, tp1)

    // (TopicPartition, Source) tuples should be issued
    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    sink.cancel()
  }

  it should "after revoke request offset for remaining partition" in assertAllStagesStopped {
    val dummy = new Dummy()

    var assertGetOffsetsOnAssign: Set[TopicPartition] => Unit = { _ =>
      ()
    }

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      assertGetOffsetsOnAssign(tps)
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    val sink = Consumer
      .committablePartitionedManualOffsetSource(consumerSettings(dummy),
                                                Subscriptions.topics(topic),
                                                getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    // assign 1
    assertGetOffsetsOnAssign = { tps =>
      tps should contain.allOf(tp0, tp1)
    }
    dummy.assignWithCallback(tp0, tp1)

    // (TopicPartition, Source) tuples should be issued
    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    // revoke tp1
    assertGetOffsetsOnAssign = { tps =>
      tps should contain only tp0
    }
    dummy.assignWithCallback(tp0)
    subSources1(tp1).runWith(Sink.ignore).futureValue should be(Done)

    sink.cancel()
  }

  it should "seek to given offset" in assertAllStagesStopped {
    val dummy = new Dummy()

    var assertGetOffsetsOnAssign: Set[TopicPartition] => Unit = { _ =>
      ()
    }

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      assertGetOffsetsOnAssign(tps)
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    val sink = Consumer
      .committablePartitionedManualOffsetSource(consumerSettings(dummy),
                                                Subscriptions.topics(topic),
                                                getOffsetsOnAssign)
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    // assign 1
    assertGetOffsetsOnAssign = { tps =>
      // this fails as of #570
      tps should contain.allOf(tp0, tp1)
    }
    dummy.assignWithCallback(tp0, tp1)

    eventually {
      dummy.seeks should contain.allOf(tp0 -> 300L, tp1 -> 300L)
    }

    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    sink.cancel()
  }

  it should "call onRevoke callback" in assertAllStagesStopped {
    val dummy = new Dummy()

    def getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] = { tps =>
      log.debug(s"getOffsetsOnAssign (${tps.mkString(",")})")
      Future.successful(tps.map(tp => (tp, 300L)).toMap)
    }

    var revoked = Vector[TopicPartition]()

    val sink = Consumer
      .committablePartitionedManualOffsetSource(consumerSettings(dummy),
                                                Subscriptions.topics(topic),
                                                getOffsetsOnAssign,
                                                onRevoke = { tp =>
                                                  revoked = revoked ++ tp
                                                })
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    dummy.assignWithCallback(tp0)
    eventually {
      revoked should contain theSameElementsInOrderAs Seq(tp1)
    }

    dummy.assignWithCallback()
    eventually {
      revoked should contain theSameElementsInOrderAs Seq(tp1, tp0)
    }

    sink.cancel()
  }

  "plain partitioned source" should "issue sources and complete them" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink = Consumer
      .plainPartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    val subSources1 = Map(sink.requestNext(), sink.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    dummy.assignWithCallback(tp0)
    subSources1(tp1).runWith(Sink.ignore).futureValue should be(Done)

    dummy.assignWithCallback()
    subSources1(tp0).runWith(Sink.ignore).futureValue should be(Done)

    sink.cancel()
  }

  it should "simulate consumer group" in assertAllStagesStopped {
    val dummy = new Dummy()
    val dummy2 = new Dummy()

    val sink1 = Consumer
      .plainPartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    val subSources1 = Map(sink1.requestNext(), sink1.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    // simulate partition re-balance
    val sink2 = Consumer
      .plainPartitionedSource(consumerSettings(dummy2), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.assignWithCallback(tp0)
    subSources1(tp1).runWith(Sink.ignore).futureValue should be(Done)

    dummy2.assignWithCallback(tp1)

    val subSources2 = Map(sink2.requestNext())
    subSources2.keys should contain only tp1

    sink1.cancel()
    sink2.cancel()
  }

  it should "issue elements" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink1 = Consumer
      .plainPartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    val subSources1 = Map(sink1.requestNext(), sink1.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    val probeTp0 = subSources1(tp0).runWith(TestSink[ConsumerRecord[K, V]]())
    val probeTp1 = subSources1(tp1).runWith(TestSink[ConsumerRecord[K, V]]())

    // trigger demand
    probeTp0.request(1L)
    probeTp1.request(1L)
    eventually {
      dummy.tpsPaused should be(Symbol("empty"))
    }
    // make records available and get expect them
    dummy.setNextPollData(tp0 -> singleRecord, tp1 -> singleRecord)
    probeTp0.expectNext().value should be("value")
    probeTp1.expectNext().value should be("value")
    // no demand anymore should lead to paused partition
    eventually {
      dummy.tpsPaused should contain.allOf(tp0, tp1)
    }

    sink1.cancel()
  }

  it should "issue in-flight elements when rebalancing" in assertAllStagesStopped {
    val dummy = new Dummy()

    val sink1 = Consumer
      .plainPartitionedSource(consumerSettings(dummy), Subscriptions.topics(topic))
      .runWith(TestSink())

    dummy.started.futureValue should be(Done)

    dummy.assignWithCallback(tp0, tp1)

    val subSources1 = Map(sink1.requestNext(), sink1.requestNext())
    subSources1.keys should contain.allOf(tp0, tp1)

    val probeTp0 = subSources1(tp0).runWith(TestSink[ConsumerRecord[K, V]]())
    val probeTp1 = subSources1(tp1).runWith(TestSink[ConsumerRecord[K, V]]())

    // trigger demand
    probeTp0.request(1L)
    probeTp1.request(1L)
    eventually {
      dummy.tpsPaused should be(Symbol("empty"))
    }

    dummy.setNextPollData(tp0 -> singleRecord, tp1 -> singleRecord)
    // let a Poll slip in
    sleep(200.millis)

    // emulate a rebalance
    dummy.assignWithCallback(tp0)
    // make records available and get expect them
    probeTp0.expectNext().value should be("value")
    probeTp1.expectNext().value should be("value")
    probeTp1.expectComplete()

    sink1.cancel()
  }

}

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

  class Dummy(val name: String = s"dummy_${ConsumerDummy.instanceCounter.incrementAndGet()}")
      extends ConsumerDummy[K, V] {
    import ConsumerDummy._

    var tps: AtomicReference[Map[TopicPartition, TpState]] = new AtomicReference(Map.empty)
    var callbacks: ConsumerRebalanceListener = null
    val emptyPollData = Map.empty[TopicPartition, java.util.List[ConsumerRecord[K, V]]]
    val nextPollData: AtomicReference[Map[TopicPartition, java.util.List[ConsumerRecord[K, V]]]] =
      new AtomicReference(emptyPollData)
    var seeks = Map[TopicPartition, Long]()

    def assignWithCallback(partitions: TopicPartition*): Unit = {
      // revoke all

      // tps can be changed from other threads (that run KafkaConsumerActor), therefore update atomically and keep
      // the actual previous value that was used for successful update
      val previousAssignment = tps.getAndUpdate(unary(_ => partitions.map(_ -> Assigned).toMap))

      // See ConsumerCoordinator.onJoinPrepare
      callbacks.onPartitionsRevoked(previousAssignment.keys.toList.asJava)

      // See ConsumerCoordinator.onJoinComplete
      callbacks.onPartitionsAssigned(partitions.asJava)
    }

    def setNextPollData(tpRecord: (TopicPartition, java.util.List[ConsumerRecord[K, V]])*): Unit = {
      logger.debug(s"data available for ${tpRecord.toMap.keys.mkString(", ")}")
      nextPollData.set(tpRecord.toMap)
    }

    def tpsRevoked: Set[TopicPartition] = tps.get.filter(_._2 == Revoked).keys.toSet
    def tpsAssigned: Set[TopicPartition] = tps.get.filter(_._2 == Assigned).keys.toSet
    def tpsResumed: Set[TopicPartition] = tps.get.filter(_._2 == Resumed).keys.toSet
    def tpsPaused: Set[TopicPartition] = tps.get.filter(_._2 == Paused).keys.toSet

    override def assignment(): java.util.Set[TopicPartition] = tps.get.filter(_._2 != Revoked).keys.toSet.asJava
    override def subscribe(topics: java.util.Collection[String], callback: ConsumerRebalanceListener): Unit =
      callbacks = callback
    override def assign(partitions: java.util.Collection[TopicPartition]): Unit =
      tps.set(partitions.asScala.map(_ -> Assigned).toMap)

    override def poll(timeout: java.time.Duration): ConsumerRecords[K, V] = {
      val data = nextPollData.get()
      val (data2, dataPaused) = data.partition {
        case (tp, _) => tpsResumed.contains(tp)
      }
      nextPollData.set(dataPaused)
      if (dataPaused.nonEmpty) {
        logger.debug(s"data for paused partitions $dataPaused")
      }
      if (data2.nonEmpty) {
        logger.debug(s"poll result $data2")
      }
      new ConsumerRecords[K, V](data2.asJava)
    }
    override def position(partition: TopicPartition): Long = 0
    override def position(partition: TopicPartition, timeout: java.time.Duration): Long = 0
    override def seek(partition: TopicPartition, offset: Long): Unit = {
      logger.debug(s"seek($partition, $offset)")
      if (!assignment().contains(partition)) {
        throw new IllegalStateException(s"Seeking on partition $partition which is not currently assigned")
      }
      seeks = seeks.updated(partition, offset)
    }
    override def seek(partition: TopicPartition, offsetAndMeta: OffsetAndMetadata): Unit =
      seek(partition, offsetAndMeta.offset)
    override def paused(): java.util.Set[TopicPartition] = tpsPaused.asJava
    override def pause(partitions: java.util.Collection[TopicPartition]): Unit = {
      super.pause(partitions)
      val ps = partitions.asScala
      logger.debug(s"pausing ${ps.mkString("(", ", ", ")")}")
      tps.updateAndGet(unary(t => t ++ ps.filter(tp => t.contains(tp)).map(_ -> Paused)))
    }
    override def resume(partitions: java.util.Collection[TopicPartition]): Unit = {
      val ps = partitions.asScala
      logger.debug(s"resuming ${ps.mkString("(", ", ", ")")}")
      tps.updateAndGet(unary(t => t ++ ps.filter(tp => t.contains(tp)).map(_ -> Resumed)))
    }
  }

  private def unary[T](f: T => T) = new UnaryOperator[T] {
    override def apply(t: T): T = f(t)
  }

}
