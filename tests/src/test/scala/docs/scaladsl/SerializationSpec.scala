/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.charset.StandardCharsets

import akka.Done
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.kafka._
import akka.kafka.internal.TestFrameworkInterface
import akka.kafka.scaladsl._
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.avro.util.Utf8
import org.apache.kafka.common.TopicPartition
// #imports
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.specific.SpecificRecord
// #imports
import net.manub.embeddedkafka.schemaregistry.{
  EmbeddedKafkaConfigWithSchemaRegistryImpl,
  EmbeddedKafkaWithSchemaRegistry
}
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.{AvroRuntimeException, Schema}
// #imports
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
// #imports
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpecLike, Matchers}
import org.slf4j.bridge.SLF4JBridgeHandler

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._

class SerializationSpec
    extends KafkaSpec(KafkaPorts.ScalaAvroSerialization)
    with FlatSpecLike
    with TestFrameworkInterface.Scalatest
    with Matchers
    with ScalaFutures
    with Eventually {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(15, Millis)))

  val schemaRegistryPort = KafkaPorts.ScalaAvroSerialization + 2

  val configWithSchemaRegistryImpl = EmbeddedKafkaConfigWithSchemaRegistryImpl(kafkaPort,
                                                                               zooKeeperPort,
                                                                               schemaRegistryPort,
                                                                               Map.empty,
                                                                               Map.empty,
                                                                               Map.empty)

  override def bootstrapServers = s"localhost:${KafkaPorts.ScalaAvroSerialization}"

  def schemaRegistryUrl = s"http://localhost:$schemaRegistryPort"

  override def setUp(): Unit = {
    // Schema registry uses Glassfish which uses java.util.logging
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
    //
    EmbeddedKafkaWithSchemaRegistry.start()(configWithSchemaRegistryImpl)
    super.setUp()
  }

  override def cleanUp(): Unit = {
    EmbeddedKafkaWithSchemaRegistry.stop()
    super.cleanUp()
  }

  "With SchemaRegistry" should "Avro serialization/deserialization work" in assertAllStagesStopped {
    val group = createGroupId()
    val topic = createTopic()

    // #serializer #de-serializer

    val kafkaAvroSerDeConfig = Map[String, Any] {
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl
    }
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

    val sample = new SampleAvroClass("key", "name")
    val samples = immutable.Seq(sample, sample, sample)
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
        .toMat(Sink.seq)(Keep.both)
        .run()
    // #de-serializer

    control.isShutdown.futureValue should be(Done)
    result.futureValue should have size samples.size.toLong
  }

}

case class SampleAvroClass(var key: String, var name: String) extends SpecificRecordBase {

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
              |{"namespace": "akka.kafka.testing",
              | "type": "record",
              | "name": "SampleAvroClass",
              | "fields": [
              |     {"name": "key", "type": "string"},
              |     {"name": "name", "type": "string"}
              | ]
              |}
            """.stripMargin)
}
