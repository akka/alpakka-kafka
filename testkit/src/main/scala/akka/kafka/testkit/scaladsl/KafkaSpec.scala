/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.testkit.internal.{KafkaTestKit, KafkaTestKitChecks}
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer.{ProducerRecord, Producer => KProducer}
import org.apache.kafka.common.ConsumerGroupState
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

abstract class KafkaSpec(_kafkaPort: Int, val zooKeeperPort: Int, actorSystem: ActorSystem)
    extends TestKit(actorSystem)
    with KafkaTestKit {

  def kafkaPort: Int = _kafkaPort

  def this(kafkaPort: Int) = this(kafkaPort, kafkaPort + 1, ActorSystem("Spec"))

  val log: Logger = LoggerFactory.getLogger(getClass)

  // used by the .log(...) stream operator
  implicit val adapter: LoggingAdapter = new Slf4jToAkkaLoggingAdapter(log)

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val scheduler: akka.actor.Scheduler = system.scheduler

  var testProducer: KProducer[String, String] = _

  def setUp(): Unit = {
    testProducer = Await.result(producerDefaults.createKafkaProducerAsync(), 2.seconds)
    setUpAdminClient()
  }

  def cleanUp(): Unit = {
    if (testProducer ne null) testProducer.close(Duration.ofSeconds(60))
    cleanUpAdminClient()
    TestKit.shutdownActorSystem(system)
  }

  def sleep(time: FiniteDuration, msg: String = ""): Unit = {
    log.debug(s"sleeping $time $msg")
    Thread.sleep(time.toMillis)
  }

  def sleepQuietly(time: FiniteDuration): Unit =
    Thread.sleep(time.toMillis)

  def awaitMultiple[T](d: FiniteDuration, futures: Future[T]*): Seq[T] =
    Await.result(Future.sequence(futures), d)

  def sleepAfterProduce: FiniteDuration = 4.seconds

  def awaitProduce(futures: Future[Done]*): Unit = {
    awaitMultiple(4.seconds, futures: _*)
    sleep(sleepAfterProduce, "to be sure producing has happened")
  }

  val partition0 = 0

  /**
   * Periodically checks if a given predicate on cluster state holds.
   *
   * If the predicate does not hold after configured amount of time, throws an exception.
   */
  def waitUntilCluster()(
      predicate: DescribeClusterResult => Boolean
  ): Unit =
    KafkaTestKitChecks.waitUntilCluster(settings.clusterTimeout, settings.checkInterval, adminClient, predicate, log)

  /**
   * Periodically checks if the given predicate on consumer group state holds.
   *
   * If the predicate does not hold after configured amount of time, throws an exception.
   */
  def waitUntilConsumerGroup(groupId: String)(predicate: ConsumerGroupDescription => Boolean): Unit =
    KafkaTestKitChecks.waitUntilConsumerGroup(groupId,
                                              settings.consumerGroupTimeout,
                                              settings.checkInterval,
                                              adminClient,
                                              predicate,
                                              log)

  /**
   * Periodically checks if the given predicate on consumer summary holds.
   *
   * If the predicate does not hold after configured amount of time, throws an exception.
   */
  def waitUntilConsumerSummary(groupId: String)(predicate: PartialFunction[List[MemberDescription], Boolean]): Unit =
    waitUntilConsumerGroup(groupId) { group =>
      group.state() == ConsumerGroupState.STABLE &&
      Try(predicate(group.members().asScala.toList)).getOrElse(false)
    }

  def createTopics(topics: Int*): immutable.Seq[String] = {
    val topicNames = topics.toList.map { number =>
      createTopicName(number)
    }
    val configs = new util.HashMap[String, String]()
    val newTopics = topicNames.map { topicName =>
      new NewTopic(topicName, 1, 1.toShort).configs(configs)
    }
    val createResult = adminClient.createTopics(newTopics.asJava)
    createResult.all().get(10, TimeUnit.SECONDS)
    topicNames
  }

  def periodicalCheck[T](description: String, maxTries: Int, sleepInBetween: FiniteDuration)(
      data: () => T
  )(predicate: T => Boolean) =
    KafkaTestKitChecks.periodicalCheck(description, maxTries * sleepInBetween, sleepInBetween)(data)(predicate)(log)

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
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

  /**
   * Produce messages to topic using specified range and return
   * a Future so the caller can synchronize consumption.
   */
  def produce(topic: String, range: Range, settings: ProducerSettings[String, String]): Future[Done] =
    Source(range)
      .map(n => new ProducerRecord(topic, partition0, DefaultKey, n.toString))
      .runWith(Producer.plainSink(settings))

  def produceTimestamped(topic: String, timestampedRange: immutable.Seq[(Int, Long)]): Future[Done] =
    Source(timestampedRange)
      .map {
        case (n, ts) => new ProducerRecord(topic, partition0, ts, DefaultKey, n.toString)
      }
      .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

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
      .map(_.value)
      .toMat(TestSink.probe)(Keep.both)
      .run()

}

private[kafka] class Slf4jToAkkaLoggingAdapter(logger: Logger) extends LoggingAdapter {
  override def isErrorEnabled: Boolean = logger.isErrorEnabled
  override def isWarningEnabled: Boolean = logger.isWarnEnabled
  override def isInfoEnabled: Boolean = logger.isInfoEnabled
  override def isDebugEnabled: Boolean = logger.isDebugEnabled
  override protected def notifyError(message: String): Unit = logger.error(message)
  override protected def notifyError(cause: Throwable, message: String): Unit = logger.error(message, cause)
  override protected def notifyWarning(message: String): Unit = logger.warn(message)
  override protected def notifyInfo(message: String): Unit = logger.info(message)
  override protected def notifyDebug(message: String): Unit = logger.debug(message)
}
