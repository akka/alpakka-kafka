/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

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
