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

  var topic1: String = _
  var topic2: String = _
  var group1: String = _
  var group2: String = _
  val partition0 = 0

  override def beforeEach(): Unit = {
    topic1 = "topic1-" + uuid
    topic2 = "topic2-" + uuid
    group1 = "group1-" + uuid
    group2 = "group2-" + uuid
  }

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def givenInitializedTopic(): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic1, partition0, null: Array[Byte], InitialMsg))
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
    "produce to plainSink and consume from plainSource" in {
      givenInitializedTopic()

      Await.result(produce(topic1, 1 to 100), remainingOrDefault)

      val consumerSettings = createConsumerSettings(group1)
      val probe = createProbe(consumerSettings, topic1)

      probe
        .request(100)
        .expectNextN((1 to 100).map(_.toString))

      probe.cancel()
    }

    // FIXME flaky test: https://github.com/akka/reactive-kafka/issues/203
    "resume consumer from committed offset" ignore {
      givenInitializedTopic()

      // NOTE: If no partition is specified but a key is present a partition will be chosen
      // using a hash of the key. If neither key nor partition is present a partition
      // will be assigned in a round-robin fashion.

      Source(1 to 100)
        .map(n => new ProducerRecord(topic1, partition0, null: Array[Byte], n.toString))
        .runWith(Producer.plainSink(producerSettings))

      val committedElement = new AtomicInteger(0)

      val consumerSettings = createConsumerSettings(group1)

      val (control, probe1) = Consumer.committableSource(consumerSettings, TopicSubscription(Set(topic1)))
        .filterNot(_.record.value == InitialMsg)
        .mapAsync(10) { elem =>
          elem.committableOffset.commitScaladsl().map { _ =>
            committedElement.set(elem.record.value.toInt)
            Done
          }
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()

      probe1
        .request(25)
        .expectNextN(25).toSet should be(Set(Done))

      probe1.cancel()
      Await.result(control.isShutdown, remainingOrDefault)

      val probe2 = Consumer.committableSource(consumerSettings, TopicSubscription(Set(topic1)))
        .map(_.record.value)
        .runWith(TestSink.probe)

      // Note that due to buffers and mapAsync(10) the committed offset is more
      // than 26, and that is not wrong

      // some concurrent publish
      Source(101 to 200)
        .map(n => new ProducerRecord(topic1, partition0, null: Array[Byte], n.toString))
        .runWith(Producer.plainSink(producerSettings))

      probe2
        .request(100)
        .expectNextN(((committedElement.get + 1) to 100).map(_.toString))

      probe2.cancel()

      // another consumer should see all
      val probe3 = Consumer.committableSource(consumerSettings.withGroupId(group2), TopicSubscription(Set(topic1)))
        .filterNot(_.record.value == InitialMsg)
        .map(_.record.value)
        .runWith(TestSink.probe)

      probe3
        .request(100)
        .expectNextN((1 to 100).map(_.toString))

      probe3.cancel()
    }

    "handle commit without demand" in {
      givenInitializedTopic()

      // important to use more messages than the internal buffer sizes
      // to trigger the intended scenario
      Await.result(produce(topic1, 1 to 100), remainingOrDefault)

      val consumerSettings = createConsumerSettings(group1)

      val (control, probe1) = Consumer.committableSource(consumerSettings, TopicSubscription(Set(topic1)))
        .filterNot(_.record.value == InitialMsg)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // request one, only
      probe1.request(1)

      val committableOffset = probe1.expectNext().committableOffset

      // enqueue some more
      Await.result(produce(topic1, 101 to 110), remainingOrDefault)

      probe1.expectNoMsg(200.millis)

      // then commit, which triggers a new poll while we haven't drained
      // previous buffer
      val done1 = committableOffset.commitScaladsl()

      Await.result(done1, remainingOrDefault)

      probe1.request(1)
      val done2 = probe1.expectNext().committableOffset.commitScaladsl()
      Await.result(done2, remainingOrDefault)

      probe1.cancel()
      Await.result(control.isShutdown, remainingOrDefault)
    }

    "consume and commit in batches" in {
      givenInitializedTopic()

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

      // Resume consumption
      val probe2 = createProbe(consumerSettings, topic1)
      val element = probe2.request(1).expectNext()

      Assertions.assert(element.toInt > 1, "Should start after first element")
      probe2.cancel()
    }

    "connect consumer to producer and commit in batches" in {
      givenInitializedTopic()

      Await.result(produce(topic1, 1 to 100), remainingOrDefault)

      val consumerSettings1 = createConsumerSettings(group1)

      val source = Consumer.committableSource(consumerSettings1, TopicSubscription(Set(topic1)))
        .map(msg =>
          {
            ProducerMessage.Message(
              // Produce to topic2
              new ProducerRecord[Array[Byte], String](topic2, msg.record.value),
              msg.committableOffset
            )
          })
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(producerSettings.parallelism)(_.commitScaladsl())

      val probe = source.runWith(TestSink.probe)

      probe.request(1).expectNext()

      probe.cancel()
    }
  }
}
