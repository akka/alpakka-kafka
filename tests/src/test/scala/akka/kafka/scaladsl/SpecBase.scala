/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

// #testkit
import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Inside, Matchers, WordSpecLike}

abstract class SpecBase(kafkaPort: Int)
    extends ScalatestKafkaSpec(kafkaPort)
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually

// #testkit

// #embeddedkafka
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import net.manub.embeddedkafka.EmbeddedKafkaConfig

class EmbeddedKafkaSampleSpec extends SpecBase(kafkaPort = 1234) with EmbeddedKafkaLike {

  // if a specific Kafka broker configuration is desired
  override def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
      zooKeeperPort,
      Map(
        "offsets.topic.replication.factor" -> "1",
        "offsets.retention.minutes" -> "1",
        "offsets.retention.check.interval.ms" -> "100"
      ))

  // ...
}
// #embeddedkafka
// #testcontainers
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike

class TestcontainersSampleSpec extends SpecBase(kafkaPort = -1) with TestcontainersKafkaLike {
  // ...
}
// #testcontainers
