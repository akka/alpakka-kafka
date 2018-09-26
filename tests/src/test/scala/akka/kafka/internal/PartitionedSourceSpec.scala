/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage._
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{CommitTimeoutException, ConsumerSettings, Subscriptions}
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.mockito
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object PartitionedSourceSpec {
  type K = String
  type V = String
  type Record = ConsumerRecord[K, V]

  val topic = "topic"

  def createMessage(seed: Int): CommittableMessage[K, V] = createMessage(seed, "topic")

  def createMessage(seed: Int,
                    topic: String,
                    groupId: String = "group1",
                    metadata: String = ""): CommittableMessage[K, V] = {
    val offset = PartitionOffset(GroupTopicPartition(groupId, topic, 1), seed.toLong)
    val record = new ConsumerRecord(offset.key.topic, offset.key.partition, offset.offset, seed.toString, seed.toString)
    CommittableMessage(record, ConsumerStage.CommittableOffsetImpl(offset, metadata)(null))
  }

  def toRecord(msg: CommittableMessage[K, V]): ConsumerRecord[K, V] = msg.record
}

class PartitionedSourceSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

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
      pollInterval = 10.millis,
      pollTimeout = 10.millis,
      1.second,
      closeTimeout = 500.millis,
      1.second,
      5.seconds,
      3,
      Duration.Inf,
      "akka.kafka.default-dispatcher",
      1.second,
      true,
      100.millis
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


  "partitioned source" should "correctly handle partition assignments and revokes" in assertAllStagesStopped {
    trait TpState
    object Paused extends TpState
    object Resumed extends TpState

    val dummy = new ConsumerDummy[K, V] {
      var tps: Map[TopicPartition, TpState] = Map.empty
      var callbacks: ConsumerRebalanceListener = null

      override def assignment(): java.util.Set[TopicPartition] = tps.keySet.asJava
      override def subscribe(topics: java.util.Collection[String], callback: ConsumerRebalanceListener): Unit =
        callbacks = callback
      override def assign(partitions: java.util.Collection[TopicPartition]): Unit = {
        callbacks.onPartitionsRevoked(tps.keySet.asJavaCollection)
        Thread.sleep(1000) // Wait for revoke asyncCB to happen
        tps = partitions.asScala.map(_ -> Resumed).toMap
        callbacks.onPartitionsAssigned(partitions)
        Thread.sleep(1000) // Wait for assignment to propagate to KafkaConsumerActor
      }
      override def poll(timeout: Long): ConsumerRecords[K, V] = new ConsumerRecords[K, V](java.util.Collections.emptyMap[TopicPartition, java.util.List[ConsumerRecord[K, V]]])
      override def position(partition: TopicPartition): Long = 0
      override def paused(): java.util.Set[TopicPartition] = tps.filter(_._2 == Paused).keySet.asJava
      override def pause(partitions: java.util.Collection[TopicPartition]): Unit =
        tps = tps ++ partitions.asScala.map(_ -> Paused)
      override def resume(partitions: java.util.Collection[TopicPartition]): Unit =
        tps = tps ++ partitions.asScala.map(_ -> Resumed)
    }
    val sinkQueue = createCommittablePartitionedSource(dummy)
      .flatMapMerge(breadth = 10, _._2)
      .runWith(Sink.queue())

    Thread.sleep(1000) // Wait for stream to materialize
    dummy.paused().asScala should be('empty)
    dummy.assign(Set(new TopicPartition(topic, 0), new TopicPartition(topic, 1)).asJavaCollection)
    dummy.paused().asScala should be('empty)
    dummy.assign(Set(new TopicPartition(topic, 0)).asJavaCollection)
    dummy.paused().asScala should be('empty)
    sinkQueue.cancel()
  }
}
