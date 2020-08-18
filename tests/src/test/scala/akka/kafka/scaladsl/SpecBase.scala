/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.Repeated
import akka.kafka.tests.scaladsl.LogCapturing
// #testkit
import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

abstract class SpecBase(kafkaPort: Int)
    extends ScalatestKafkaSpec(kafkaPort)
    with WordSpecLike
    with Matchers
    with ScalaFutures
    // #testkit
    with LogCapturing
    with IntegrationPatience
    with Repeated
    // #testkit
    with Eventually {

  protected def this() = this(kafkaPort = -1)
}

// #testkit

// #testcontainers
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike

class TestcontainersSampleSpec extends SpecBase with TestcontainersKafkaLike {
  // ...
}
// #testcontainers

// #testcontainers-settings
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike

class TestcontainersNewSettingsSampleSpec extends SpecBase with TestcontainersKafkaPerClassLike {

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system)
    .withNumBrokers(3)
    .withInternalTopicsReplicationFactor(2)
    .withConfigureKafka { brokerContainers =>
      brokerContainers.foreach(_.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"))
    }

  // ...
}
// #testcontainers-settings
