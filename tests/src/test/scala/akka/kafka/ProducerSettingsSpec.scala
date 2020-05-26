/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import akka.actor.ActorSystem
import akka.kafka.tests.scaladsl.LogCapturing
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

class ProducerSettingsSpec
    extends WordSpecLike
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with LogCapturing {

  "ProducerSettings" must {

    "handle serializers defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.producer")
      val settings = ProducerSettings(conf, None, None)
      settings.properties("bootstrap.servers") should ===("localhost:9092")
    }

    "handle serializers passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val settings = ProducerSettings(conf, new ByteArraySerializer, new StringSerializer)
      settings.properties("bootstrap.servers") should ===("localhost:9092")
    }

    "handle key serializer passed as args config and value serializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.producer")
      val settings = ProducerSettings(conf, Some(new ByteArraySerializer), None)
      settings.properties("bootstrap.servers") should ===("localhost:9092")
    }

    "handle value serializer passed as args config and key serializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.producer")
      val settings = ProducerSettings(conf, None, Some(new ByteArraySerializer))
      settings.properties("bootstrap.servers") should ===("localhost:9092")
    }

    "filter passwords from kafka-clients properties" in {
      val conf = ConfigFactory.load().getConfig(ProducerSettings.configPath)
      val settings = ProducerSettings(conf, new ByteArraySerializer, new StringSerializer)
        .withProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "hemligt")
        .withProperty("ssl.truststore.password", "geheim")
      val s = settings.toString
      s should include(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
      s should not include ("hemligt")
      s should include("ssl.truststore.password")
      s should not include ("geheim")
    }

    "throw IllegalArgumentException if no value serializer defined" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException] {
        ProducerSettings(conf, None, None)
      }
      exception.getMessage should ===(
        "requirement failed: Value serializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no value serializer defined (null case). Key serializer passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException] {
        ProducerSettings(conf, new ByteArraySerializer, null)
      }
      exception.getMessage should ===(
        "requirement failed: Value serializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no value serializer defined (null case). Key serializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException] {
        ProducerSettings(conf, None, null)
      }
      exception.getMessage should ===(
        "requirement failed: Value serializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no key serializer defined" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException] {
        ProducerSettings(conf, None, None)
      }
      exception.getMessage should ===(
        "requirement failed: Key serializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no key serializer defined (null case). Value serializer passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException] {
        ProducerSettings(conf, null, new ByteArraySerializer)
      }
      exception.getMessage should ===(
        "requirement failed: Key serializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no key serializer defined (null case). Value serializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException] {
        ProducerSettings(conf, null, None)
      }
      exception.getMessage should ===(
        "requirement failed: Key serializer should be defined or declared in configuration"
      )
    }

  }

  "Discovery" should {
    val config = ConfigFactory
      .parseString(ProducerSettingsSpec.DiscoveryConfigSection)
      .withFallback(ConfigFactory.load())
      .resolve()

    "use enriched settings for consumer creation" in {
      implicit val actorSystem = ActorSystem("test", config)

      // #discovery-settings
      import akka.kafka.scaladsl.DiscoverySupport

      val producerConfig = config.getConfig("discovery-producer")
      val settings = ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
        .withEnrichAsync(DiscoverySupport.producerBootstrapServers(producerConfig))
      // #discovery-settings

      val exception = settings.createKafkaProducerAsync()(actorSystem.dispatcher).failed.futureValue
      exception shouldBe a[org.apache.kafka.common.KafkaException]
      exception.getCause shouldBe a[org.apache.kafka.common.config.ConfigException]
      exception.getCause.getMessage shouldBe "No resolvable bootstrap urls given in bootstrap.servers"
      TestKit.shutdownActorSystem(actorSystem)
    }

    "fail if using non-async creation with enrichAsync" in {
      implicit val actorSystem = ActorSystem("test", config)

      import akka.kafka.scaladsl.DiscoverySupport

      val producerConfig = config.getConfig("discovery-producer")
      val settings = ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
        .withEnrichAsync(DiscoverySupport.producerBootstrapServers(producerConfig))

      val exception = intercept[IllegalStateException] {
        settings.createKafkaProducer()
      }
      exception shouldBe a[IllegalStateException]
      TestKit.shutdownActorSystem(actorSystem)
    }
  }

}

object ProducerSettingsSpec {
  val DiscoveryConfigSection =
    s"""
        // #discovery-service
        discovery-producer: $${akka.kafka.producer} {
          service-name = "kafkaService1"
          resolve-timeout = 10 ms
        }
        // #discovery-service
        akka.discovery.method = config
        akka.discovery.config.services = {
          kafkaService1 = {
            endpoints = [
              { host = "cat", port = 1233 }
              { host = "dog", port = 1234 }
            ]
          }
        }
        """
}
