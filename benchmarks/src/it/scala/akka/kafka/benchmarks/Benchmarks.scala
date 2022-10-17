/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.BenchmarksBase._
import akka.kafka.benchmarks.InflightMetrics._
import akka.kafka.benchmarks.PerfFixtureHelpers.FilledTopic
import akka.kafka.benchmarks.Timed.{runPerfTest, runPerfTestInflightMetrics}
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import com.typesafe.config.Config

object BenchmarksBase {
  // Message count multiplier to adapt for shorter local testing
  val factor = 1000

  // Default settings for Kafka testcontainers cluster
  var settings: Option[KafkaTestkitTestcontainersSettings] = None
  def initialize(config: Config): Unit = {
    settings = settings.orElse(Some(KafkaTestkitTestcontainersSettings(config)))
  }
  def numBrokers: Int =
    settings.getOrElse {
      throw new RuntimeException("Call initialize first")
    }.numBrokers

  lazy val topic_50_100 = FilledTopic(50 * factor, 100, replicationFactor = numBrokers)

  lazy val topic_100_100 = FilledTopic(100 * factor, 100, replicationFactor = numBrokers)
  lazy val topic_100_5000 = FilledTopic(100 * factor, 5000, replicationFactor = numBrokers)

  lazy val topic_1000_100 = FilledTopic(1000 * factor, 100, replicationFactor = numBrokers)
  lazy val topic_1000_5000 = FilledTopic(1000 * factor, 5 * 1000, replicationFactor = numBrokers)
  lazy val topic_1000_5000_8 =
    FilledTopic(msgCount = 1000 * factor, msgSize = 5 * 1000, numberOfPartitions = 8, replicationFactor = numBrokers)
  lazy val topic_1000_5000_100 =
    FilledTopic(msgCount = 1000 * factor, msgSize = 5 * 1000, numberOfPartitions = 100, replicationFactor = numBrokers)

  lazy val topic_2000_100 = FilledTopic(2000 * factor, 100, replicationFactor = numBrokers)
  lazy val topic_2000_500 = FilledTopic(2000 * factor, 500, replicationFactor = numBrokers)
  lazy val topic_2000_5000 = FilledTopic(2000 * factor, 5000, replicationFactor = numBrokers)
  lazy val topic_2000_5000_8 = FilledTopic(2000 * factor, 5000, numberOfPartitions = 8, replicationFactor = numBrokers)
}

abstract class BenchmarksBase() extends SpecBase with TestcontainersKafkaLike {

  override def setUp(): Unit = {
    BenchmarksBase.initialize(system.settings.config.getConfig(KafkaTestkitTestcontainersSettings.ConfigPath))
    super.setUp()
  }
}

class ApacheKafkaConsumerNokafka extends BenchmarksBase() {
  it should "bench" in {
    val cmd = RunTestCommand("apache-kafka-plain-consumer-nokafka", bootstrapServers, topic_2000_100)
    runPerfTest(cmd, KafkaConsumerFixtures.noopFixtureGen(cmd), KafkaConsumerBenchmarks.consumePlainNoKafka)
  }
}

class AlpakkaKafkaConsumerNokafka extends BenchmarksBase() {
  it should "bench" in {
    val cmd = RunTestCommand("alpakka-kafka-plain-consumer-nokafka", bootstrapServers, topic_2000_100)
    runPerfTest(cmd,
                ReactiveKafkaConsumerFixtures.noopFixtureGen(cmd),
                ReactiveKafkaConsumerBenchmarks.consumePlainNoKafka)
  }
}

class ApacheKafkaPlainConsumer extends BenchmarksBase() {
  it should "bench" in {
    val cmd = RunTestCommand("apache-kafka-plain-consumer", bootstrapServers, topic_2000_100)
    runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumePlain)
  }

  it should "bench with normal messages and one hundred partitions" in {
    val cmd =
      RunTestCommand("apache-kafka-plain-consumer-normal-msg-100-partitions", bootstrapServers, topic_1000_5000_100)
    runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumePlain)
  }
}

class AlpakkaKafkaPlainConsumer extends BenchmarksBase() {
  it should "bench" in {
    val cmd = RunTestCommand("alpakka-kafka-plain-consumer", bootstrapServers, topic_2000_100)
    runPerfTest(cmd, ReactiveKafkaConsumerFixtures.plainSources(cmd), ReactiveKafkaConsumerBenchmarks.consumePlain)
  }

  it should "bench with normal messages and one hundred partitions with inflight metrics" in {
    val cmd =
      RunTestCommand("alpakka-kafka-plain-consumer-normal-msg-100-partitions-with-inflight-metrics",
                     bootstrapServers,
                     topic_1000_5000_100)
    val consumerMetricNames = List[ConsumerMetricRequest](
      ConsumerMetricRequest("bytes-consumed-total", CounterMetricType),
      ConsumerMetricRequest("fetch-rate", GaugeMetricType),
      ConsumerMetricRequest("fetch-total", CounterMetricType),
      ConsumerMetricRequest("records-per-request-avg", GaugeMetricType),
      ConsumerMetricRequest("records-consumed-total", CounterMetricType)
    )
    val brokerMetricNames: List[BrokerMetricRequest] = List(
      BrokerMetricRequest(s"kafka.server:type=BrokerTopicMetrics,name=TotalFetchRequestsPerSec",
                          topic_1000_5000_100.topic,
                          "Count",
                          CounterMetricType),
      BrokerMetricRequest(s"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec",
                          topic_1000_5000_100.topic,
                          "Count",
                          CounterMetricType)
    )
    val brokerJmxUrls = brokerContainers.map(_.getJmxServiceUrl).toList
    runPerfTestInflightMetrics(
      cmd,
      consumerMetricNames,
      brokerMetricNames,
      brokerJmxUrls,
      ReactiveKafkaConsumerFixtures.plainSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumePlainInflightMetrics
    )
  }
}

class ApacheKafkaAtMostOnceConsumer extends BenchmarksBase() {
  it should "bench" in {
    val cmd = RunTestCommand("apache-kafka-at-most-once-consumer", bootstrapServers, topic_50_100)
    runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumeCommitAtMostOnce)
  }
}

class AlpakkaKafkaAtMostOnceConsumer extends BenchmarksBase() {
  it should "bench" in {
    val cmd = RunTestCommand("alpakka-kafka-at-most-once-consumer", bootstrapServers, topic_50_100)
    runPerfTest(cmd,
                ReactiveKafkaConsumerFixtures.committableSources(cmd),
                ReactiveKafkaConsumerBenchmarks.consumeCommitAtMostOnce)
  }
}
