/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.kafka.scaladsl

import akka.kafka._
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class ConnectionCheckerSpec extends SpecBase with TestcontainersKafkaPerClassLike {

  override def setUp(): Unit = ()

  override def cleanUp(): Unit = Try(super.cleanUp())

  override def startCluster(): String = {
    val bootstrapServers = super.startCluster()
    testProducer = Await.result(producerDefaults.createKafkaProducerAsync(), 2.seconds)
    setUpAdminClient()
    bootstrapServers
  }

  override val testcontainersSettings =
    KafkaTestkitTestcontainersSettings(system)
      .withInternalTopicsReplicationFactor(1)
      .withConfigureKafka { brokerContainers =>
        brokerContainers.foreach {
          _.withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "200")
        }
      }

  val retryInterval: FiniteDuration = 100.millis
  val connectionCheckerConfig: ConnectionCheckerSettings = ConnectionCheckerSettings(1, retryInterval, 2d)
  val noBrokerConsumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(s"no-broker:1234")
      .withConnectionChecker(connectionCheckerConfig)
      .withGroupId("KafkaConnectionCheckerSpec")
      .withMetadataRequestTimeout(1.seconds)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  val failingDetectionTime: FiniteDuration = 10.seconds

  val topic = "superAwesomeTopic"

  "PlainSource" must {

    "fail stream and control.isShutdown when kafka down" in assertAllStagesStopped {
      val (control, futDone) =
        Consumer.plainSource(noBrokerConsumerSettings, Subscriptions.topics(topic)).toMat(Sink.ignore)(Keep.both).run()

      Await.ready(control.isShutdown.zip(futDone), failingDetectionTime)
    }

    "fail stream and control.isShutdown when kafka down and not recover during max retries exceeded" in assertAllStagesStopped {
      startCluster()

      val msg = "hello"
      produceString(topic, scala.collection.immutable.Seq(msg))

      val consumerSettings = noBrokerConsumerSettings.withBootstrapServers(bootstrapServers)

      val (control, probe) =
        Consumer.plainSource(consumerSettings, Subscriptions.topics(topic)).toMat(TestSink())(Keep.both).run()

      probe.ensureSubscription().requestNext().value() shouldBe msg

      stopCluster()
      Await.ready(control.isShutdown, failingDetectionTime)
      probe.request(1).expectError().getClass shouldBe classOf[KafkaConnectionFailed]
    }
  }

}
