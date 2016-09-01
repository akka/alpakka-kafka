/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.scalatest._

class ProducerSettingsSpec extends WordSpecLike with Matchers {

  "ProducerSettings" must {

    "handle serializers defined in config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
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
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val settings = ProducerSettings(conf, Some(new ByteArraySerializer), None)
      settings.properties("bootstrap.servers") should ===("localhost:9092")
    }

    "handle value serializer passed as args config and key serializer defined in config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val settings = ProducerSettings(conf, None, Some(new ByteArraySerializer))
      settings.properties("bootstrap.servers") should ===("localhost:9092")
    }

    "throw IllegalArgumentException if no value serializer defined" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException]{
        ProducerSettings(conf, None, None)
      }
      exception.getMessage should ===("requirement failed: Value serializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no value serializer defined (null case). Key serializer passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException]{
        ProducerSettings(conf, new ByteArraySerializer, null)
      }
      exception.getMessage should ===("requirement failed: Value serializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no value serializer defined (null case). Key serializer defined in config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.key.serializer = org.apache.kafka.common.serialization.StringSerializer
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException]{
        ProducerSettings(conf, None, null)
      }
      exception.getMessage should ===("requirement failed: Value serializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no key serializer defined" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException]{
        ProducerSettings(conf, None, None)
      }
      exception.getMessage should ===("requirement failed: Key serializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no key serializer defined (null case). Value serializer passed as args config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException]{
        ProducerSettings(conf, null, new ByteArraySerializer)
      }
      exception.getMessage should ===("requirement failed: Key serializer should be defined or declared in configuration")
    }

    "throw IllegalArgumentException if no key serializer defined (null case). Value serializer defined in config" in {
      val conf = ConfigFactory.parseString("""
        akka.kafka.producer.kafka-clients.bootstrap.servers = "localhost:9092"
        akka.kafka.producer.kafka-clients.parallelism = 1
        akka.kafka.producer.kafka-clients.value.serializer = org.apache.kafka.common.serialization.StringSerializer
        """).withFallback(ConfigFactory.load()).getConfig("akka.kafka.producer")
      val exception = intercept[IllegalArgumentException]{
        ProducerSettings(conf, null, None)
      }
      exception.getMessage should ===("requirement failed: Key serializer should be defined or declared in configuration")
    }

  }

}
