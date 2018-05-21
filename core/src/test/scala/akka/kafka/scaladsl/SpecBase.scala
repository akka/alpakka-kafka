/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.test.Utils._
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest._
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.collection.immutable

abstract class SpecBase(val kafkaPort: Int, val zooKeeperPort: Int, actorSystem: ActorSystem)
  extends TestKit(actorSystem)
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with Eventually {

  def this(kafkaPort: Int) = this(kafkaPort, kafkaPort + 1, ActorSystem("Spec"))

  implicit val stageStoppingTimeout: StageStoppingTimeout = StageStoppingTimeout(15.seconds)
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = createKafkaConfig

  def createKafkaConfig: EmbeddedKafkaConfig

  def bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"

  var testProducer: KafkaProducer[String, String] = _

  val DefaultKey = "key"
  val InitialMsg =
    "initial msg in topic, required to create the topic before any consumer subscribes to it"

  val producerDefaults =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  val consumerDefaults = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withWakeupTimeout(10 seconds)
    .withMaxWakeups(10)

  override protected def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    testProducer = producerDefaults.createKafkaProducer()
  }

  override def afterAll(): Unit = {
    testProducer.close(60, TimeUnit.SECONDS)
    TestKit.shutdownActorSystem(system)
    EmbeddedKafka.stop()
  }

  private val topicCounter = new AtomicInteger()

  def createTopic(number: Int) = s"topic-$number-${topicCounter.incrementAndGet()}"

  def createGroup(number: Int) = s"group-$number-${topicCounter.incrementAndGet()}"

  val partition0 = 0

  def givenInitializedTopic(topic: String): Unit =
    testProducer.send(new ProducerRecord(topic, partition0, DefaultKey, InitialMsg))

  /**
   * Produce messages to topic using specified range and return
   * a Future so the caller can synchronize consumption.
   */
  def produce(topic: String, range: immutable.Seq[Int], partition: Int = partition0): Future[Done] =
    produceString(topic, range.map(_.toString), partition)

  def produceString(topic: String, range: immutable.Seq[String], partition: Int = partition0): Future[Done] =
    Source(range)
      // NOTE: If no partition is specified but a key is present a partition will be chosen
      // using a hash of the key. If neither key nor partition is present a partition
      // will be assigned in a round-robin fashion.
      .map(n => new ProducerRecord(topic, partition, DefaultKey, n))
      .runWith(Producer.plainSink(producerDefaults, testProducer))

  /**
   * Produce messages to topic using specified range and return
   * a Future so the caller can synchronize consumption.
   */
  def produce(topic: String, range: Range, settings: ProducerSettings[String, String]): Future[Done] =
    Source(range)
      .map(n => new ProducerRecord(topic, partition0, DefaultKey, n.toString))
      .runWith(Producer.plainSink(settings))

  /**
   * Produce batches over several topics.
   */
  def produceBatches(topics: Seq[String], batches: Int, batchSize: Int): Future[Seq[Done]] = {
    val produceMessages: immutable.Seq[Future[Done]] = (0 until batches)
      .flatMap { batch =>
        topics.map { topic =>
          val batchStart = batch * batchSize
          val values = (batchStart until batchStart + batchSize).map(i => topic + i.toString)
          produceString(topic, values, partition = partition0)
        }
      }
    Future.sequence(produceMessages)
  }

  /**
   * Messages expected from #produceBatches generation.
   */
  def batchMessagesExpected(topics: Seq[String], batches: Int, batchSize: Int): (Seq[String], Long) = {
    val expectedData = topics.flatMap { topic =>
      (0 until batches * batchSize).map(i => topic + i.toString)
    }
    val expectedCount = batches * batchSize * topics.length
    (expectedData, expectedCount.toLong)
  }

  def createProbe(consumerSettings: ConsumerSettings[String, String], topic: String*): (Control, TestSubscriber.Probe[String]) =
    Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic.toSet))
      .filterNot(_.value == InitialMsg)
      .map(_.value)
      .toMat(TestSink.probe)(Keep.both)
      .run()

}
