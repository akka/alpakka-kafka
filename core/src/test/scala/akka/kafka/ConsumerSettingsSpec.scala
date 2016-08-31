/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
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
      val settings = ConsumerSettings(conf, Some(new ByteArrayDeserializer), Some(new StringDeserializer))
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
      settings.getProperty("client.id") should ===("client1")
      settings.getProperty("foo") should ===("bar")
      settings.getProperty("bootstrap.foo") should ===("baz")
    }

    "handle deserializers defined in config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.value.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val settings = ConsumerSettings(conf)
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
    }

    "handle deserializers passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.parallelism = 1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val settings = ConsumerSettings(conf, Some(new ByteArrayDeserializer), Some(new StringDeserializer))
      settings.getProperty("bootstrap.servers") should ===("localhost:9092")
    }

    "throw IllegalArgumentException if no value deserializer defined in" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.key.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException]{
        ConsumerSettings(conf)
      }
      exception.getMessage should ===("requirement failed: Value deserializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no key deserializer defined in" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.value.deserializer = org.apache.kafka.common.serialization.StringDeserializer
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException]{
        ConsumerSettings(conf)
      }
      exception.getMessage should ===("requirement failed: Key deserializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no value deserializer defined as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException]{
        ConsumerSettings(conf, Some(new ByteArrayDeserializer), None)
      }
      exception.getMessage should ===("requirement failed: Value deserializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no key deserializer defined as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.consumer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.consumer.kafka-clients.client.id = client1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.consumer")
      val exception = intercept[IllegalArgumentException]{
        ConsumerSettings(conf, None, Some(new StringDeserializer))
      }
      exception.getMessage should ===("requirement failed: Key deserializer should be defined or declared in configuration")
    }

  }

}
