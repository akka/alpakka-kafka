/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.BenchmarksBase.{topic_2000_100, topic_2000_500, topic_2000_5000, topic_2000_5000_8}
import akka.kafka.benchmarks.Timed.runPerfTest
import akka.kafka.benchmarks.app.RunTestCommand

class ApacheKafkaPlainProducer extends BenchmarksBase() {
  private val prefix = "apache-kafka-plain-producer"

  it should "bench with small messages" in {
    val cmd = RunTestCommand(prefix, bootstrapServers, topic_2000_100.freshTopic)
    runPerfTest(cmd, KafkaProducerFixtures.initializedProducer(cmd), KafkaProducerBenchmarks.plainFlow)
  }

  it should "bench with 500b messages" in {
    val cmd = RunTestCommand(prefix + "-500b", bootstrapServers, topic_2000_500.freshTopic)
    runPerfTest(cmd, KafkaProducerFixtures.initializedProducer(cmd), KafkaProducerBenchmarks.plainFlow)
  }

  it should "bench with normal messages" in {
    val cmd = RunTestCommand(prefix + "-normal-msg", bootstrapServers, topic_2000_5000.freshTopic)
    runPerfTest(cmd, KafkaProducerFixtures.initializedProducer(cmd), KafkaProducerBenchmarks.plainFlow)
  }

  it should "bench with normal messages written to 8 partitions" in {
    val cmd =
      RunTestCommand(prefix + "-normal-msg-8-partitions", bootstrapServers, topic_2000_5000_8.freshTopic)
    runPerfTest(cmd, KafkaProducerFixtures.initializedProducer(cmd), KafkaProducerBenchmarks.plainFlow)
  }
}

class AlpakkaKafkaPlainProducer extends BenchmarksBase() {
  private val prefix = "alpakka-kafka-plain-producer"

  it should "bench with small messages" in {
    val cmd = RunTestCommand(prefix, bootstrapServers, topic_2000_100.freshTopic)
    runPerfTest(cmd, ReactiveKafkaProducerFixtures.flowFixture(cmd), ReactiveKafkaProducerBenchmarks.plainFlow)
  }

  it should "bench with 500b messages" in {
    val cmd = RunTestCommand(prefix + "-500b", bootstrapServers, topic_2000_500.freshTopic)
    runPerfTest(cmd, ReactiveKafkaProducerFixtures.flowFixture(cmd), ReactiveKafkaProducerBenchmarks.plainFlow)
  }

  it should "bench with normal messages" in {
    val cmd = RunTestCommand(prefix + "-normal-msg", bootstrapServers, topic_2000_5000.freshTopic)
    runPerfTest(cmd, ReactiveKafkaProducerFixtures.flowFixture(cmd), ReactiveKafkaProducerBenchmarks.plainFlow)
  }

  it should "bench with normal messages written to 8 partitions" in {
    val cmd =
      RunTestCommand(prefix + "-normal-msg-8-partitions", bootstrapServers, topic_2000_5000_8.freshTopic)
    runPerfTest(cmd, ReactiveKafkaProducerFixtures.flowFixture(cmd), ReactiveKafkaProducerBenchmarks.plainFlow)
  }
}

