/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util
import java.util.{Arrays, Properties}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import kafka.admin.{AdminClient => OldAdminClient}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, DescribeClusterResult, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

trait EmbeddedKafkaLike extends KafkaSpec {

  lazy implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = createKafkaConfig
  def createKafkaConfig: EmbeddedKafkaConfig

  override def bootstrapServers =
    s"localhost:${embeddedKafkaConfig.kafkaPort}"

  override def setUp(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    super.setUp()
  }

  override def cleanUp(): Unit = {
    EmbeddedKafka.stop()
    super.cleanUp()
  }
}

abstract class KafkaSpec(val kafkaPort: Int, val zooKeeperPort: Int, actorSystem: ActorSystem)
    extends TestKit(actorSystem) {

  def this(kafkaPort: Int) = this(kafkaPort, kafkaPort + 1, ActorSystem("Spec"))

  def log: Logger = LoggerFactory.getLogger(getClass)

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  def bootstrapServers: String

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

  val adminDefaults = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config
  }

  def setUp(): Unit =
    testProducer = producerDefaults.createKafkaProducer()

  def cleanUp(): Unit = {
    testProducer.close(60, TimeUnit.SECONDS)
    TestKit.shutdownActorSystem(system)
  }

  def sleep(time: FiniteDuration): Unit = {
    log.debug(s"sleeping $time")
    Thread.sleep(time.toMillis)
  }

  def awaitMultiple[T](d: FiniteDuration, futures: Future[T]*): Seq[T] =
    Await.result(Future.sequence(futures), d)

  def sleepAfterProduce: FiniteDuration = 4.seconds

  def awaitProduce(futures: Future[Done]*): Unit = {
    awaitMultiple(4.seconds, futures: _*)
    sleep(sleepAfterProduce)
  }

  private val topicCounter = new AtomicInteger()

  def createTopicName(number: Int) = s"topic-$number-${topicCounter.incrementAndGet()}"

  def createGroupId(number: Int = 0) = s"group-$number-${topicCounter.incrementAndGet()}"

  def createTransactionalId(number: Int = 0) = s"transactionalId-$number-${topicCounter.incrementAndGet()}"

  val partition0 = 0

  def givenInitializedTopic(topic: String): Unit =
    testProducer.send(new ProducerRecord(topic, partition0, DefaultKey, InitialMsg))

  def adminClient(): AdminClient =
    AdminClient.create(adminDefaults)

  /**
   * Get an old admin client which is deprecated. However only this client allows access
   * to consumer group summaries
   *
   */
  def oldAdminClient(): OldAdminClient =
    OldAdminClient.create(adminDefaults)

  /**
   * Periodically checks if a given predicate on cluster state holds.
   *
   * If the predicate does not hold after `maxTries`, throws an exception.
   */
  def waitUntilCluster(maxTries: Int = 10, sleepInBetween: FiniteDuration = 100.millis)(
      predicate: DescribeClusterResult => Boolean
  ): Unit = {
    @tailrec def checkCluster(triesLeft: Int): Unit = {
      val cluster = adminClient().describeCluster()
      if (!predicate(cluster)) {
        if (triesLeft > 0) {
          sleep(sleepInBetween)
          checkCluster(triesLeft - 1)
        } else {
          throw new Error("Failure while waiting for desired cluster state")
        }
      }
    }

    checkCluster(maxTries)
  }

  /**
   * Periodically checks if a given predicate on consumer group state holds.
   *
   * If the predicate does not hold after `maxTries`, throws an exception.
   */
  def waitUntilConsumerGroup(
      groupId: String,
      timeout: Duration = 1.second,
      maxTries: Int = 10,
      sleepInBetween: FiniteDuration = 100.millis
  )(predicate: kafka.admin.AdminClient#ConsumerGroupSummary => Boolean) = {
    @tailrec def checkConsumerGroup(triesLeft: Int): Unit = {
      val consumerGroup = oldAdminClient().describeConsumerGroup(groupId, timeout.toMillis)
      if (!predicate(consumerGroup)) {
        if (triesLeft > 0) {
          sleep(sleepInBetween)
          checkConsumerGroup(triesLeft - 1)
        } else {
          throw new Error("Failure while waiting for desired consumer group state")
        }
      }
    }

    checkConsumerGroup(maxTries)
  }

  /**
   * Create a topic with given partinion number and replication factor.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(number: Int = 0, partitions: Int = 1, replication: Int = 1): String = {
    val topicName = createTopicName(number)

    val configs = new util.HashMap[String, String]()
    val createResult = adminClient().createTopics(
      Arrays.asList(new NewTopic(topicName, partitions, replication.toShort).configs(configs))
    )
    createResult.all().get(10, TimeUnit.SECONDS)
    topicName
  }

  def createTopics(topics: Int*): immutable.Seq[String] = {
    val topicNames = topics.toList.map { number =>
      createTopicName(number)
    }
    val configs = new util.HashMap[String, String]()
    val newTopics = topicNames.map { topicName =>
      new NewTopic(topicName, 1, 1.toShort).configs(configs)
    }
    val createResult = adminClient().createTopics(newTopics.asJava)
    createResult.all().get(10, TimeUnit.SECONDS)
    topicNames
  }

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

  def produceTimestamped(topic: String,
                         timestampedRange: immutable.Seq[(Int, Long)],
                         partiion: Int = partition0): Future[Done] =
    Source(timestampedRange)
      .map {
        case (n, ts) => new ProducerRecord(topic, partition0, ts, DefaultKey, n.toString)
      }
      .runWith(Producer.plainSink(producerDefaults, testProducer))

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

  def createProbe(consumerSettings: ConsumerSettings[String, String],
                  topic: String*): (Control, TestSubscriber.Probe[String]) =
    Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic.toSet))
      .filterNot(_.value == InitialMsg)
      .map(_.value)
      .toMat(TestSink.probe)(Keep.both)
      .run()

}
