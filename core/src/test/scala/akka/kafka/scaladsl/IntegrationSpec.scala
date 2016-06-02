/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.{ ConsumerSettings, ProducerSettings }
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.ProducerMessage
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer }
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike }

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
  var group1: String = _
  var client1: String = _
  val partition0 = 0

  override def beforeEach(): Unit = {
    topic1 = "topic1-" + uuid
    group1 = "group1-" + uuid
    client1 = "client1-" + uuid
  }

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def givenInitializedTopic(): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic1, partition0, null: Array[Byte], InitialMsg))
    producer.close(60, TimeUnit.SECONDS)
  }

  def produceMessages(range: Range) = {
    Source(range)
      .map(n => new ProducerRecord(topic1, partition0, null: Array[Byte], n.toString))
      .runWith(Producer.plainSink(producerSettings))
  }

  def createConsumerSettings() = {
    ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, Set(topic1))
      .withBootstrapServers("localhost:9092")
      .withGroupId(group1)
      .withClientId(client1)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  "Reactive kafka streams" must {
    "produce to plainSink and consume from plainSource" in {
      givenInitializedTopic()

      produceMessages(1 to 100)

      val consumerSettings = createConsumerSettings()

      val probe = Consumer.plainSource(consumerSettings)
        // it's not 100% sure we get the first message, see https://issues.apache.org/jira/browse/KAFKA-3334
        .filterNot(_.value == InitialMsg)
        .map(_.value)
        .runWith(TestSink.probe)

      probe
        .request(100)
        .expectNextN((1 to 100).map(_.toString))

      probe.cancel()
    }

    "resume consumer from committed offset" ignore {
      givenInitializedTopic()

      // NOTE: If no partition is specified but a key is present a partition will be chosen
      // using a hash of the key. If neither key nor partition is present a partition
      // will be assigned in a round-robin fashion.

      Source(1 to 100)
        .map(n => new ProducerRecord(topic1, partition0, null: Array[Byte], n.toString))
        .runWith(Producer.plainSink(producerSettings))

      val committedElement = new AtomicInteger(0)

      val consumerSettings = createConsumerSettings()

      val (control, probe1) = Consumer.committableSource(consumerSettings)
        .filterNot(_.value == InitialMsg)
        .mapAsync(10) { elem =>
          elem.committableOffset.commitScaladsl().map { _ =>
            committedElement.set(elem.value.toInt)
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

      val probe2 = Consumer.committableSource(consumerSettings)
        .map(_.value)
        .runWith(TestSink.probe)

      // Note that due to buffers and mapAsync(10) the committed offset is more
      // than 26, and that is not wrong

      // some concurrent publish
      Source(101 to 200)
        .map(n => new ProducerRecord(topic1, partition0, null: Array[Byte], n.toString))
        .runWith(Producer.plainSink(producerSettings))

      probe2
        .request(100)
        // FIXME this sometimes fails on Travis with
        //       "timeout (10 seconds) during expectMsg while waiting for OnNext(36)"
        //       and therefore this test is currently ignored. Must be investigated.
        .expectNextN(((committedElement.get + 1) to 100).map(_.toString))

      probe2.cancel()
    }

    "handle commit without demand" in {
      givenInitializedTopic()

      // important to use more messages than the internal buffer sizes
      // to trigger the intended scenario
      produceMessages(1 to 100)

      val consumerSettings = createConsumerSettings()

      val (control, probe1) = Consumer.committableSource(consumerSettings)
        .filterNot(_.value == InitialMsg)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // request one, only
      probe1.request(1)

      val committableOffset = probe1.expectNext().committableOffset

      // enqueue some more
      produceMessages(101 to 110)

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

      produceMessages(1 to 100)

      val consumerSettings = createConsumerSettings()

      def consumeAndBatchCommit() = {
        Consumer.committableSource(consumerSettings)
          .map { msg => msg.committableOffset }
          .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
            batch.updated(elem)
          }
          .mapAsync(1)(_.commitScaladsl()).toMat(TestSink.probe)(Keep.both)
      }

      consumeAndBatchCommit()

      val probe = Consumer.plainSource(consumerSettings)
        .filterNot(_.value == InitialMsg)
        .map(_.value)
        .runWith(TestSink.probe)

      probe
        .request(100)
        .expectNextN((1 to 100).map(_.toString))

      produceMessages(101 to 150)

      consumeAndBatchCommit()

      probe
        .request(50)
        .expectNextN((101 to 150).map(_.toString))

      probe.cancel()
    }

    "connect consumer to producer and commit in batches" in {
      givenInitializedTopic()

      produceMessages(1 to 100)

      val consumerSettings = createConsumerSettings()

      Consumer.committableSource(consumerSettings.withClientId("client1"))
        .map(msg =>
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(producerSettings.parallelism)(_.commitScaladsl())

    }
  }

}

