/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

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
