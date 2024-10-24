/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import akka.kafka.internal.ConfigSettings
import akka.kafka.tests.scaladsl.LogCapturing
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConfigSettingsSpec extends AnyWordSpec with Matchers with LogCapturing {

  "ConfigSettings" must {

    "handle nested properties" in {
      val conf = ConfigFactory
        .parseString(
          """
        kafka-client.bootstrap.servers = "localhost:9092"
        kafka-client.bootstrap.foo = baz
        kafka-client.foo = bar
        kafka-client.client.id = client1
        """
        )
        .withFallback(ConfigFactory.load())
        .getConfig("kafka-client")
      val settings = ConfigSettings.parseKafkaClientsProperties(conf)
      settings("bootstrap.servers") should ===("localhost:9092")
      settings("client.id") should ===("client1")
      settings("foo") should ===("bar")
      settings("bootstrap.foo") should ===("baz")
    }
  }
}
