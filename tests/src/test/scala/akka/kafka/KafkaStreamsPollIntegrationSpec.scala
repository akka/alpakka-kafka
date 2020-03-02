/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{Codecs, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.Deserializer
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PartialFunctionValues, WordSpecLike}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
final class KafkaStreamsPollIntegrationSpec
    extends TestKit(ActorSystem("KafkaStreamsPollIntegrationSpec", ConfigFactory.load("testing")))
    with WordSpecLike
    with PartialFunctionValues
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with EmbeddedKafka
    with ScalaFutures {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[KafkaStreamsPollIntegrationSpec])
  private val materializerSettings = ActorMaterializerSettings(system)
  private implicit val materializer = ActorMaterializer(materializerSettings)
  private implicit val ec = system.dispatcher

  private val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
  private val serializer = Codecs.stringSerializer
  private val deserializer = Codecs.stringDeserializer

  "Alpakka kafka" should {
    "poll more than twice" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualKafkaConfig: EmbeddedKafkaConfig =>
        val topic = "topic"
        // write some simple string data
        val records = (1 to 10).map(x => (s"key-$x", s"value-$x"))
        publishToKafka(topic, records)(config = actualKafkaConfig, keySerializer = serializer, serializer = serializer)
        // make sure we are not crazy and the data is present
        implicit val deserialize = deserializer.asInstanceOf[Deserializer[AnyRef]]
        consumeNumberMessagesFrom(topic, 10) should have size 10

        val consumerConf = system.settings.config.getConfig("akka.kafka.consumer")
        val settings = ConsumerSettings(consumerConf, deserializer, deserializer)
          .withBootstrapServers(s"localhost:${actualKafkaConfig.kafkaPort}")
          .withProperty("auto.offset.reset", "earliest")
          .withProperty("max.poll.records", "2")
          .withGroupId(s"foo-bar-${UUID.randomUUID()}")
          .withStopTimeout(0.seconds)

        val subscription = Subscriptions.topics(topic)
        val source = Consumer.plainSource(settings, subscription)

        val (control, probe) = source
          .log("peek")
          .map(_.value)
          .toMat(TestSink.probe)(Keep.both)
          .run()
        probe.request(10)
        for (i <- Range(1, 11)) {
          println(s"Expecting $i ....")
          println(s"    -> ${probe.expectNext(10.seconds)}")
        }
        probe.cancel()
        control.isShutdown.futureValue shouldBe Done
      }
    }
  }
}
