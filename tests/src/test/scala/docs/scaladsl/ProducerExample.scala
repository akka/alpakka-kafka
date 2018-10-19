/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.kafka.{KafkaPorts, ProducerMessage, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import akka.Done
import akka.kafka.ProducerMessage.MultiResultPart
import net.manub.embeddedkafka.EmbeddedKafkaConfig

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

class ProducerExample extends DocsSpecBase(KafkaPorts.ScalaTransactionsExamples) {
  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort, zooKeeperPort)

  override def sleepAfterProduce: FiniteDuration = 4.seconds
  private def waitBeforeValidation(): Unit = sleep(6.seconds)

  "PlainSink" should "work" in {
    // #settings
    val config = system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings =
      ProducerSettings(config, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)
    // #settings
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val topic = createTopic()
    // #plainSink
    val done: Future[Done] =
      Source(1 to 100)
        .map(_.toString)
        .map(value => new ProducerRecord[String, String](topic, value))
        .runWith(Producer.plainSink(producerSettings))
    // #plainSink
    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .toMat(Sink.seq)(Keep.both)
      .run()
    waitBeforeValidation()
    done.futureValue should be(Done)
    control2.shutdown().futureValue should be(Done)
    result.futureValue should have size (100)
  }

  "PlainSink with shared producer" should "work" in {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val producerSettings = producerDefaults
    val kafkaProducer = producerSettings.createKafkaProducer()
    val topic = createTopic()
    // #plainSinkWithProducer
    val done = Source(1 to 100)
      .map(_.toString)
      .map(value => new ProducerRecord[String, String](topic, value))
      .runWith(Producer.plainSink(producerSettings, kafkaProducer))
    // #plainSinkWithProducer
    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .toMat(Sink.seq)(Keep.both)
      .run()
    done.futureValue should be(Done)
    waitBeforeValidation()
    control2.shutdown().futureValue should be(Done)
    result.futureValue should have size (100)
    kafkaProducer.close()
  }

  "Metrics" should "be observed" in {
    // #producer
    val config = system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings =
      ProducerSettings(config, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)
    val kafkaProducer = producerSettings.createKafkaProducer()
    // #producer
    // #producerMetrics
    val metrics: java.util.Map[org.apache.kafka.common.MetricName, _ <: org.apache.kafka.common.Metric] =
      kafkaProducer.metrics() // observe metrics
    // #producerMetrics
    metrics.isEmpty should be(false)
    // #producer

    // using the kafkaProducer

    kafkaProducer.close()
    // #producer
  }

  def createMessage[KeyType, ValueType, PassThroughType](key: KeyType, value: ValueType, passThrough: PassThroughType) =
    // #singleMessage
    ProducerMessage.single(
      new ProducerRecord("topicName", key, value),
      passThrough
    )
  // #singleMessage

  def createMultiMessage[KeyType, ValueType, PassThroughType](key: KeyType,
                                                              value: ValueType,
                                                              passThrough: PassThroughType) = {
    import scala.collection.immutable
    // #multiMessage
    ProducerMessage.multi(
      immutable.Seq(
        new ProducerRecord("topicName", key, value),
        new ProducerRecord("anotherTopic", key, value)
      ),
      passThrough
    )
    // #multiMessage
  }

  def createPassThroughMessage[KeyType, ValueType, PassThroughType](key: KeyType,
                                                                    value: ValueType,
                                                                    passThrough: PassThroughType) =
    // format:off
    // #passThroughMessage
    ProducerMessage.passThrough(
      passThrough
    )
  // #passThroughMessage
  // format:on

  "flexiFlow" should "work" in {
    val producerSettings = producerDefaults
    val topic = createTopic()
    def println(s: String): Unit = {}
    // format:off
    // #flow
    val done = Source(1 to 100)
      .map { number =>
        val partition = 0
        val value = number.toString
        ProducerMessage.single(
          new ProducerRecord(topic, partition, "key", value),
          number
        )
      }
      .via(Producer.flexiFlow(producerSettings))
      .map {
        case ProducerMessage.Result(metadata, message) =>
          val record = message.record
          s"${metadata.topic}/${metadata.partition} ${metadata.offset}: ${record.value}"

        case ProducerMessage.MultiResult(parts, passThrough) =>
          parts
            .map {
              case MultiResultPart(metadata, record) =>
                s"${metadata.topic}/${metadata.partition} ${metadata.offset}: ${record.value}"
            }
            .mkString(", ")

        case ProducerMessage.PassThroughResult(passThrough) =>
          s"passed through"
      }
      .runWith(Sink.foreach(println(_)))
    // #flow
    // format:on
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .toMat(Sink.seq)(Keep.both)
      .run()
    done.futureValue should be(Done)
    waitBeforeValidation()
    control2.shutdown().futureValue should be(Done)
    result.futureValue should have size (100)
  }
}
