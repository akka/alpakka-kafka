/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.kafka.ProducerMessage.MultiResultPart
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.kafka.{ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import scala.concurrent.duration._

class ProducerExample extends DocsSpecBase with TestcontainersKafkaLike {

  private def waitBeforeValidation(): Unit = sleep(6.seconds)

  val invalidTopicName = "---*---"

  "Creating a producer" should "work" in {
    // #producer
    // #settings
    val config = system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings =
      ProducerSettings(config, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)
    // #settings
    val kafkaProducer: Future[org.apache.kafka.clients.producer.Producer[String, String]] =
      producerSettings.createKafkaProducerAsync()

    // using the kafka producer

    kafkaProducer.foreach(p => p.close())
    // #producer
  }

  "PlainSink" should "work" in assertAllStagesStopped {
    val config = system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings =
      ProducerSettings(config, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)
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

  "PlainSink with shared producer" should "work" in assertAllStagesStopped {
    val consumerSettings = consumerDefaults.withGroupId(createGroupId())
    val producerSettings = producerDefaults
    val topic = createTopic()
    // #plainSinkWithProducer
    // create a producer
    val kafkaProducer = producerSettings.createKafkaProducer()
    val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

    val done = Source(1 to 100)
      .map(_.toString)
      .map(value => new ProducerRecord[String, String](topic, value))
      .runWith(Producer.plainSink(settingsWithProducer))
    // #plainSinkWithProducer
    val (control2, result) = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .toMat(Sink.seq)(Keep.both)
      .run()
    done.futureValue should be(Done)
    waitBeforeValidation()
    control2.shutdown().futureValue should be(Done)
    result.futureValue should have size (100)
    // #plainSinkWithProducer

    // close the producer after use
    kafkaProducer.close()
    // #plainSinkWithProducer
  }

  "Metrics" should "be observed" in assertAllStagesStopped {
    val config = system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings =
      ProducerSettings(config, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)
    producerSettings
      .createKafkaProducerAsync()
      .map { kafkaProducer =>
        // #producerMetrics
        val metrics: java.util.Map[org.apache.kafka.common.MetricName, _ <: org.apache.kafka.common.Metric] =
          kafkaProducer.metrics() // observe metrics
        // #producerMetrics
        metrics.isEmpty should be(false)
        kafkaProducer.close()
        Done
      }
      .futureValue shouldBe Done
  }

  def createMessage[KeyType, ValueType, PassThroughType](
      key: KeyType,
      value: ValueType,
      passThrough: PassThroughType
  ): ProducerMessage.Envelope[KeyType, ValueType, PassThroughType] = {
    // #singleMessage
    val single: ProducerMessage.Envelope[KeyType, ValueType, PassThroughType] =
      ProducerMessage.single(
        new ProducerRecord("topicName", key, value),
        passThrough
      )
    // #singleMessage
    single
  }

  def createMultiMessage[KeyType, ValueType, PassThroughType](
      key: KeyType,
      value: ValueType,
      passThrough: PassThroughType
  ): ProducerMessage.Envelope[KeyType, ValueType, PassThroughType] = {
    import scala.collection.immutable
    // #multiMessage
    val multi: ProducerMessage.Envelope[KeyType, ValueType, PassThroughType] =
      ProducerMessage.multi(
        immutable.Seq(
          new ProducerRecord("topicName", key, value),
          new ProducerRecord("anotherTopic", key, value)
        ),
        passThrough
      )
    // #multiMessage
    multi
  }

  def createPassThroughMessage[KeyType, ValueType, PassThroughType](
      key: KeyType,
      value: ValueType,
      passThrough: PassThroughType
  ): ProducerMessage.Envelope[KeyType, ValueType, PassThroughType] = {
    // #passThroughMessage
    val ptm: ProducerMessage.Envelope[KeyType, ValueType, PassThroughType] =
      ProducerMessage.passThrough(
        passThrough
      )
    // #passThroughMessage
    ptm
  }

  "flexiFlow" should "work" in assertAllStagesStopped {
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
        case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) =>
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

  // This showed a race fixed in https://github.com/akka/alpakka-kafka/pull/1025
  it should "fail stream with error from producing" in assertAllStagesStopped {
    val streamCompletion =
      Source
        .single(ProducerMessage.single(new ProducerRecord[String, String](invalidTopicName, "1")))
        .via(Producer.flexiFlow(producerDefaults))
        .runWith(Sink.head)

    streamCompletion.failed.futureValue shouldBe an[org.apache.kafka.common.errors.InvalidTopicException]
  }

}
