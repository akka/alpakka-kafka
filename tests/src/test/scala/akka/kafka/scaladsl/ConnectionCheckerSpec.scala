/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka.{ConnectionCheckerSettings, ConsumerSettings, KafkaConnectionFailed, KafkaPorts, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSink
import kafka.server.KafkaConfig
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class ConnectionCheckerSpec extends WordSpecLike with Matchers with LogCapturing {

  implicit val system: ActorSystem = ActorSystem("KafkaConnectionCheckerSpec")
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val mat: ActorMaterializer = ActorMaterializer()

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = KafkaPorts.KafkaConnectionCheckerTest,
    customBrokerProperties = Map(
      KafkaConfig.OffsetsTopicPartitionsProp -> "1",
      KafkaConfig.AutoCreateTopicsEnableProp -> "true",
      KafkaConfig.GroupInitialRebalanceDelayMsProp -> "200"
    )
  )

  val retryInterval: FiniteDuration = 100.millis
  val connectionCheckerConfig: ConnectionCheckerSettings = ConnectionCheckerSettings(1, retryInterval, 2d)
  val settings: ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(s"localhost:${KafkaPorts.KafkaConnectionCheckerTest}")
      .withConnectionChecker(connectionCheckerConfig)
      .withGroupId("KafkaConnectionCheckerSpec")
      .withMetadataRequestTimeout(1.seconds)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  val failingDetectionTime: FiniteDuration = 10.seconds

  val topic = "superAwesomeTopic"

  "PlainSource" must {

    "fail stream and control.isShutdown when kafka down" in {
      val (control, futDone) =
        Consumer.plainSource(settings, Subscriptions.topics(topic)).toMat(Sink.ignore)(Keep.both).run

      Await.ready(control.isShutdown.zip(futDone), failingDetectionTime)
    }

    "fail stream and control.isShutdown when kafka down and not recover during max retries exceeded" in {
      val embeddedK = EmbeddedKafka.start()

      val (control, probe) =
        Consumer.plainSource(settings, Subscriptions.topics(topic)).toMat(TestSink.probe)(Keep.both).run

      val msg = "hello"
      EmbeddedKafka.publishStringMessageToKafka(topic, msg)
      probe.ensureSubscription().requestNext().value() shouldBe msg

      embeddedK.stop(true)
      Await.ready(control.isShutdown, failingDetectionTime)
      probe.request(1).expectError().getClass shouldBe classOf[KafkaConnectionFailed]
    }
  }

}
