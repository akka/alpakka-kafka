/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.charset.StandardCharsets

import akka.Done
import akka.kafka._
import akka.kafka.scaladsl._
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.util.Utf8
import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable
import scala.concurrent.duration._
// #imports
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.specific.SpecificRecord
// #imports
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
// #imports
import org.apache.kafka.common.serialization._
// #imports
import scala.collection.JavaConverters._

// #schema-registry-settings
class SchemaRegistrySerializationSpec extends DocsSpecBase with TestcontainersKafkaPerClassLike {

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system).withSchemaRegistry(true)

  // tests..
  // #schema-registry-settings

  "With SchemaRegistry" should "Avro serialization/deserialization work" in assertAllStagesStopped {
    val group = createGroupId()
    val topic = createTopic()

    // #serializer #de-serializer

    val kafkaAvroSerDeConfig = Map[String, Any](
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl,
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> true.toString
    )
    // #serializer #de-serializer

    // #de-serializer
    val consumerSettings: ConsumerSettings[String, SpecificRecord] = {
      val kafkaAvroDeserializer = new KafkaAvroDeserializer()
      kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)
      val deserializer = kafkaAvroDeserializer.asInstanceOf[Deserializer[SpecificRecord]]

      ConsumerSettings(system, new StringDeserializer, deserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId(group)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }
    // #de-serializer

    // #serializer
    val producerSettings: ProducerSettings[String, SpecificRecord] = {
      val kafkaAvroSerializer = new KafkaAvroSerializer()
      kafkaAvroSerializer.configure(kafkaAvroSerDeConfig.asJava, false)
      val serializer = kafkaAvroSerializer.asInstanceOf[Serializer[SpecificRecord]]

      ProducerSettings(system, new StringSerializer, serializer)
        .withBootstrapServers(bootstrapServers)
    }

    val samples = (1 to 3).map(i => SampleAvroClass(s"key_$i", s"name_$i"))
    val producerCompletion =
      Source(samples)
        .map(n => new ProducerRecord[String, SpecificRecord](topic, n.key, n))
        .runWith(Producer.plainSink(producerSettings))
    // #serializer
    awaitProduce(producerCompletion)
    // #de-serializer

    val (control, result) =
      Consumer
        .plainSource(consumerSettings, Subscriptions.topics(topic))
        .take(samples.size.toLong)
        .map(_.value())
        .toMat(Sink.seq)(Keep.both)
        .run()
    // #de-serializer

    control.isShutdown.futureValue should be(Done)
    result.futureValue should contain theSameElementsInOrderAs samples
  }

  "Error in deserialization" should "signal undisguised" in assertAllStagesStopped {
    val group = createGroupId()
    val topic = createTopic()

    val producerSettings: ProducerSettings[String, Array[Byte]] =
      ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
        .withBootstrapServers(bootstrapServers)

    val samples = immutable.Seq("String1")
    val producerCompletion =
      Source(samples)
        .map(n => new ProducerRecord(topic, n, n.getBytes(StandardCharsets.UTF_8)))
        .runWith(Producer.plainSink(producerSettings))
    awaitProduce(producerCompletion)

    val (control, result) =
      Consumer
        .plainSource(specificRecordConsumerSettings(group), Subscriptions.topics(topic))
        .take(samples.size.toLong)
        .toMat(Sink.seq)(Keep.both)
        .run()

    control.isShutdown.futureValue should be(Done)
    result.failed.futureValue shouldBe a[org.apache.kafka.common.errors.SerializationException]
  }

  it should "signal to all streams of a shared actor in same request, and keep others alive" in assertAllStagesStopped {
    val group = createGroupId()
    val topic = createTopic(suffix = 0, partitions = 3)

    val consumerActor =
      system.actorOf(KafkaConsumerActor.props(specificRecordConsumerSettings(group)), "sharedKafkaConsumerActor")

    val samples = immutable.Seq("String1", "String2", "String3")

    val (control1, probe1) =
      Consumer
        .plainExternalSource[String, SpecificRecord](consumerActor,
                                                     Subscriptions.assignment(new TopicPartition(topic, 0)))
        .toMat(TestSink())(Keep.both)
        .run()

    val (control2, probe2) =
      Consumer
        .plainExternalSource[String, SpecificRecord](consumerActor,
                                                     Subscriptions.assignment(new TopicPartition(topic, 1)))
        .toMat(TestSink())(Keep.both)
        .run()

    val (thisStreamStaysAlive, probe3) =
      Consumer
        .plainExternalSource[String, SpecificRecord](consumerActor,
                                                     Subscriptions.assignment(new TopicPartition(topic, 2)))
        .toMat(TestSink())(Keep.both)
        .run()

    // request from 2 streams
    probe1.request(samples.size.toLong)
    probe2.request(samples.size.toLong)
    sleep(500.millis, "to establish demand")

    val producerSettings: ProducerSettings[String, Array[Byte]] =
      ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
        .withBootstrapServers(bootstrapServers)

    Source(samples)
      .map(n => new ProducerRecord(topic, 0, n, n.getBytes(StandardCharsets.UTF_8)))
      .runWith(Producer.plainSink(producerSettings))

    probe1.expectError() shouldBe a[org.apache.kafka.common.errors.SerializationException]
    probe2.expectError() shouldBe a[org.apache.kafka.common.errors.SerializationException]
    probe3.cancel()

    control1.isShutdown.futureValue should be(Done)
    control2.isShutdown.futureValue should be(Done)
    thisStreamStaysAlive.shutdown().futureValue should be(Done)
    consumerActor ! KafkaConsumerActor.Stop
  }

  it should "signal to sub-sources" in assertAllStagesStopped {
    val group = createGroupId()
    val topic = createTopic()

    val (control1, partitionedProbe) =
      Consumer
        .plainPartitionedSource(specificRecordConsumerSettings(group), Subscriptions.topics(topic))
        .toMat(TestSink())(Keep.both)
        .run()

    partitionedProbe.request(1L)
    val (_, subSource) = partitionedProbe.expectNext()
    val subStream = subSource.runWith(TestSink())

    subStream.request(1L)

    val producerSettings: ProducerSettings[String, Array[Byte]] =
      ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
        .withBootstrapServers(bootstrapServers)
    Source
      .single(new ProducerRecord(topic, 0, "0", "0".getBytes(StandardCharsets.UTF_8)))
      .runWith(Producer.plainSink(producerSettings))

    subStream.expectError() shouldBe a[org.apache.kafka.common.errors.SerializationException]

    control1.shutdown().futureValue should be(Done)
  }

  private def specificRecordConsumerSettings(group: String): ConsumerSettings[String, SpecificRecord] = {
    val kafkaAvroSerDeConfig = Map[String, Any] {
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl
    }
    val kafkaAvroDeserializer = new KafkaAvroDeserializer()
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)

    val deserializer = kafkaAvroDeserializer.asInstanceOf[Deserializer[SpecificRecord]]
    ConsumerSettings(system, new StringDeserializer, deserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }
  // #schema-registry-settings
}
// #schema-registry-settings

case class SampleAvroClass(var key: String, var name: String) extends SpecificRecordBase {

  def this() = this(null, null)

  override def get(i: Int): AnyRef = i match {
    case 0 => key
    case 1 => name
    case index => throw new AvroRuntimeException(s"Unknown index: $index")
  }

  override def put(i: Int, v: scala.Any): Unit = i match {
    case 0 => key = asString(v)
    case 1 => name = asString(v)
    case index => throw new AvroRuntimeException(s"Unknown index: $index")
  }

  private def asString(v: Any) =
    v match {
      case utf8: Utf8 => utf8.toString
      case _ => v.asInstanceOf[String]
    }

  override def getSchema: Schema = SampleAvroClass.SCHEMA$
}

object SampleAvroClass {
  val SCHEMA$ : Schema =
    new org.apache.avro.Schema.Parser().parse("""
                                                |{"namespace": "docs.scaladsl",
                                                | "type": "record",
                                                | "name": "SampleAvroClass",
                                                | "fields": [
                                                |     {"name": "key", "type": "string"},
                                                |     {"name": "name", "type": "string"}
                                                | ]
                                                |}
            """.stripMargin)
}
