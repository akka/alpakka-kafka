/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.scalatest._

class ConsumerSettingsSpec extends WordSpecLike with Matchers {

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

}
