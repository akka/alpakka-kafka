/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.actor.ActorSystem
import akka.kafka.tests.scaladsl.LogCapturing
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.ExecutionContext

class ConsumerSettingsSpec
    extends WordSpecLike
    with Matchers
    with OptionValues
    with ScalaFutures
    with IntegrationPatience
    with LogCapturing {

  "ConsumerSettings" must {

    "handle nested kafka-clients properties" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.bootstrap.foo = baz
        akka.kafka.consumer.kafka-clients.foo = bar
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val settings = ConsumerSettings(conf, new ByteArrayDeserializer, new StringDeserializer)
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
      settings.getProperty("client.id") should ===("client1")
      settings.getProperty("foo") should ===("bar")
      settings.getProperty("bootstrap.foo") should ===("baz")
    }

    "handle deserializers defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.value.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.consumer")
      val settings = ConsumerSettings(conf, None, None)
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
    }

    "handle deserializers passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.parallelism = 1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val settings = ConsumerSettings(conf, new ByteArrayDeserializer, new StringDeserializer)
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
    }

    "handle key deserializer passed as args config and value deserializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.value.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.consumer")
      val settings = ConsumerSettings(conf, Some(new ByteArrayDeserializer), None)
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
    }

    "handle value deserializer passed as args config and key deserializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.consumer")
      val settings = ConsumerSettings(conf, None, Some(new ByteArrayDeserializer))
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
    }

    "throw IllegalArgumentException if no value deserializer defined" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException] {
        ConsumerSettings(conf, None, None)
      }
      exception.getMessage should ===(
        "requirement failed: Value deserializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no value deserializer defined (null case). Key serializer passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException] {
        ConsumerSettings(conf, new ByteArrayDeserializer, null)
      }
      exception.getMessage should ===(
        "requirement failed: Value deserializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no value deserializer defined (null case). Key serializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException] {
        ConsumerSettings(conf, None, null)
      }
      exception.getMessage should ===(
        "requirement failed: Value deserializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no key deserializer defined" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.value.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException] {
        ConsumerSettings(conf, None, None)
      }
      exception.getMessage should ===(
        "requirement failed: Key deserializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no key deserializer defined (null case). Value serializer passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException] {
        ConsumerSettings(conf, null, new ByteArrayDeserializer)
      }
      exception.getMessage should ===(
        "requirement failed: Key deserializer should be defined or declared in configuration"
      )
    }

    "throw IllegalArgumentException if no key deserializer defined (null case). Value serializer defined in config" in {
      val conf = ConfigFactory
        .parseString(
          """
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.value.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException] {
        ConsumerSettings(conf, null, None)
      }
      exception.getMessage should ===(
        "requirement failed: Key deserializer should be defined or declared in configuration"
      )
    }

  }

  "Discovery" should {
    val config = ConfigFactory
      .parseString(ConsumerSettingsSpec.DiscoveryConfigSection)
      .withFallback(ConfigFactory.load())
      .resolve()

    "read bootstrap servers from config" in {
      import akka.kafka.scaladsl.DiscoverySupport
      implicit val actorSystem = ActorSystem("test", config)

      DiscoverySupport.bootstrapServers(config.getConfig("discovery-consumer")).futureValue shouldBe "cat:1233,dog:1234"

      TestKit.shutdownActorSystem(actorSystem)
    }

    "use enriched settings for consumer creation" in {
      implicit val actorSystem = ActorSystem("test", config)
      implicit val executionContext: ExecutionContext = actorSystem.dispatcher

      // #discovery-settings
      import akka.kafka.scaladsl.DiscoverySupport

      val consumerConfig = config.getConfig("discovery-consumer")
      val settings = ConsumerSettings(consumerConfig, new StringDeserializer, new StringDeserializer)
        .withEnrichAsync(DiscoverySupport.consumerBootstrapServers(consumerConfig))
      // #discovery-settings

      val exception = settings.createKafkaConsumerAsync().failed.futureValue
      exception shouldBe a[org.apache.kafka.common.KafkaException]
      exception.getCause shouldBe a[org.apache.kafka.common.config.ConfigException]
      exception.getCause.getMessage shouldBe "No resolvable bootstrap urls given in bootstrap.servers"

      TestKit.shutdownActorSystem(actorSystem)
    }

  }

}

object ConsumerSettingsSpec {

  val DiscoveryConfigSection =
    s"""
       // #discovery-service
      discovery-consumer: $${akka.kafka.consumer} {
        service-name = "kafkaService1"
        resolve-timeout = 10 ms
      }
      // #discovery-service
      // #discovery-with-config
      akka.discovery.method = config
      akka.discovery.config.services = {
        kafkaService1 = {
          endpoints = [
            { host = "cat", port = 1233 }
            { host = "dog", port = 1234 }
          ]
        }
      }
      // #discovery-with-config
   """
}
