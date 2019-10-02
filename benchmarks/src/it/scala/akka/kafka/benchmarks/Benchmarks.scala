/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.kafka.benchmarks.PerfFixtureHelpers.FilledTopic
import akka.kafka.benchmarks.Timed.runPerfTest
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import org.scalatest.FlatSpecLike

import BenchmarksBase._

object BenchmarksBase {
  // Message count multiplier to adapt for shorter local testing
  val factor = 1000

  val topic_50_100 = FilledTopic(50 * factor, 100)

  val topic_100_100 = FilledTopic(100 * factor, 100)
  val topic_100_5000 = FilledTopic(100 * factor, 5000)

  val topic_1000_100 = FilledTopic(1000 * factor, 100)
  val topic_1000_5000 = FilledTopic(1000 * factor, 5 * 1000)
  val topic_1000_5000_8 = FilledTopic(msgCount = 1000 * factor, msgSize = 5 * 1000, numberOfPartitions = 8)

  val topic_2000_100 = FilledTopic(2000 * factor, 100)
  val topic_2000_500 = FilledTopic(2000 * factor, 500)
  val topic_2000_5000 = FilledTopic(2000 * factor, 5000)
  val topic_2000_5000_8 = FilledTopic(2000 * factor, 5000, numberOfPartitions = 8)

}

abstract class BenchmarksBase() extends ScalatestKafkaSpec(0) with FlatSpecLike {

  override def bootstrapServers: String =
    (1 to BuildInfo.kafkaScale).map(i => sys.props(s"kafka_${i}_9094")).mkString(",")

  override def setUp(): Unit = {
    super.setUp()
    waitUntilCluster() {
      _.nodes().get().size == BuildInfo.kafkaScale
    }
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
}

class AlpakkaKafkaPlainConsumer extends BenchmarksBase() {
  it should "bench" in {
    val cmd = RunTestCommand("alpakka-kafka-plain-consumer", bootstrapServers, topic_2000_100)
    runPerfTest(cmd, ReactiveKafkaConsumerFixtures.plainSources(cmd), ReactiveKafkaConsumerBenchmarks.consumePlain)
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
