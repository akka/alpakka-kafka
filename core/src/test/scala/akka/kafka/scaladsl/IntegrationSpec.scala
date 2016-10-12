/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.Subscriptions.TopicSubscription
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.ProducerMessage
import akka.kafka.ProducerMessage.Message
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source, Sink}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.TestSubscriber
import akka.testkit.TestKit

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import org.scalatest.Assertions

class IntegrationSpec extends TestKit(ActorSystem("IntegrationSpec"))
    with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
    with ConversionCheckedTripleEquals {

  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181)
  val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  def uuid = UUID.randomUUID().toString

  /*var topic1: String = _
  var topic2: String = _
  var group1: String = _
  var group2: String = _ */

  val partition0 = 0

  /*override def beforeEach(): Unit = {
    topic1 = "topic1-" + uuid
    topic2 = "topic2-" + uuid
    group1 = "group1-" + uuid
    group2 = "group2-" + uuid
  }*/

  def createTopic(number: Int) = s"topic$number-" + uuid
  def createGroup(number: Int) = s"group$number-" + uuid

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def givenInitializedTopic(topic: String): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, null: Array[Byte], InitialMsg))
    producer.close(60, TimeUnit.SECONDS)
  }

  /**
   * Produce messages to topic using specified range and return
   * a Future so the caller can synchronize consumption.
   */
  def produce(topic: String, range: Range): Future[Done] = {
    val source = Source(range)
      .map(n => {
        val record = new ProducerRecord(topic, partition0, null: Array[Byte], n.toString)

        Message(record, NotUsed)
      })
      .viaMat(Producer.flow(producerSettings))(Keep.right)

    source.runWith(Sink.ignore)
  }

  def createConsumerSettings(group: String): ConsumerSettings[Array[Byte], String] = {
    ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(group)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withWakeupTimeout(4 seconds)
  }

  def createProbe(
    consumerSettings: ConsumerSettings[Array[Byte], String],
    topic: String
  ): TestSubscriber.Probe[String] = {
    Consumer.plainSource(consumerSettings, TopicSubscription(Set(topic)))
      .filterNot(_.value == InitialMsg)
      .map(_.value)
      .runWith(TestSink.probe)
  }

  "Reactive kafka streams" must {

    "consume and commit in batches" in {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)

      givenInitializedTopic(topic1)

      Await.result(produce(topic1, 1 to 100), remainingOrDefault)
      val consumerSettings = createConsumerSettings(group1)

      def consumeAndBatchCommit(topic: String) = {
        Consumer.committableSource(
          consumerSettings,
          TopicSubscription(Set(topic))
        )
          .map { msg => msg.committableOffset }
          .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) {
            (batch, elem) => batch.updated(elem)
          }
          .mapAsync(1)(_.commitScaladsl())
          .toMat(TestSink.probe)(Keep.both).run()
      }

      val (control, probe) = consumeAndBatchCommit(topic1)

      // Request one batch
      probe.request(1).expectNextN(1)

      probe.cancel()
      Await.result(control.isShutdown, remainingOrDefault)

      println("TEST: after Await control.isShutdown")

      // Resume consumption
      val consumerSettings2 = createConsumerSettings(group1)
      val probe2 = createProbe(consumerSettings2, topic1)

      println("probe2 created")
      val element = probe2.request(1).expectNext()

      Assertions.assert(element.toInt > 1, "Should start after first element")
      probe2.cancel()
    }
  }
}
