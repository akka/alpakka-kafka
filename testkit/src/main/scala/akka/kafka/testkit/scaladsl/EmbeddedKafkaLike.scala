/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

@deprecated("Use testcontainers instead", "2.0.4")
trait EmbeddedKafkaLike extends KafkaSpec {

  lazy implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = createKafkaConfig

  def createKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort, zooKeeperPort)

  override def bootstrapServers =
    s"localhost:${embeddedKafkaConfig.kafkaPort}"

  override def setUp(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    super.setUp()
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
    EmbeddedKafka.stop()
  }
}
