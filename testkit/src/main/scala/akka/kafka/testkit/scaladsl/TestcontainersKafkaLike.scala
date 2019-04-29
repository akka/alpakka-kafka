/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import org.testcontainers.containers.KafkaContainer

/**
 * Uses [Testcontainers](https://www.testcontainers.org/) to start a Kafka broker in a Docker container.
 * The Testcontainers dependency has to be added explicitly.
 */
trait TestcontainersKafkaLike extends KafkaSpec {
  import TestcontainersKafkaLike._

  override def kafkaPort: Int = {
    requireStarted()
    kafkaPortInternal
  }

  override def bootstrapServers: String = {
    requireStarted()
    kafkaBootstrapServersInternal
  }

  override def setUp(): Unit = {
    if (kafkaPortInternal == -1) {
      val kafkaContainer = new KafkaContainer()
      kafkaContainer.start()
      kafkaBootstrapServersInternal = kafkaContainer.getBootstrapServers
      kafkaPortInternal = kafkaBootstrapServersInternal.substring(kafkaBootstrapServersInternal.lastIndexOf(":") + 1).toInt
    }
    super.setUp()
  }
}

private object TestcontainersKafkaLike {

  private var kafkaBootstrapServersInternal: String = _
  private var kafkaPortInternal: Int = -1

  private def requireStarted(): Unit = require(kafkaPortInternal != -1, "Testcontainers Kafka hasn't been started via `setUp`")

}
