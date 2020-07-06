/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

// #embeddedkafka
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import com.github.ghik.silencer.silent
import net.manub.embeddedkafka.EmbeddedKafkaConfig

@silent
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
