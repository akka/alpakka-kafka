/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.kafka.ProducerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.BeforeAndAfterEach
import akka.Done
import akka.stream.scaladsl.Keep
import scala.concurrent.Await
import java.util.concurrent.atomic.AtomicInteger

class IntegrationSpec extends TestKit(ActorSystem("IntegrationSpec"))
    with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
    with ConversionCheckedTripleEquals {

  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
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
    .withBootstrapServers("localhost:9092")

  def givenInitializedTopic(): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic1, partition0, null: Array[Byte], InitialMsg))
    producer.close(60, TimeUnit.SECONDS)
  }

  "Reactive kafka streams" must {
    "produce to plainSink and consume from plainSource" in {
      givenInitializedTopic()

      Source(1 to 100)
        .map(n => new ProducerRecord(topic1, null: Array[Byte], n.toString))
        .runWith(Producer.plainSink(producerSettings))

      val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, Set(topic1))
        .withBootstrapServers("localhost:9092")
        .withGroupId(group1)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

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

      val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, Set(topic1))
        .withBootstrapServers("localhost:9092")
        .withGroupId(group1)
        .withClientId(client1)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val committedElement = new AtomicInteger(0)
      val (control, probe1) = Consumer.committableSource(consumerSettings)
        .filterNot(_.value == InitialMsg)
        .mapAsync(10) { elem =>
          elem.committableOffset.commit().map { _ =>
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
      Await.result(control.stopped, remainingOrDefault)

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

    // TODO add more tests

  }

}

