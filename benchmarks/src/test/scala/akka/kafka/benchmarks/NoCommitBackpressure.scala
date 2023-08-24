/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.Timed.runPerfTest
import akka.kafka.benchmarks.app.RunTestCommand

import BenchmarksBase._

class RawKafkaCommitEveryPollConsumer extends BenchmarksBase() {
  private val prefix = "apache-kafka-batched-no-pausing-"

  it should "bench with small messages" in {
    val cmd = RunTestCommand(prefix + "consumer", bootstrapServers, topic_1000_100)
    runPerfTest(cmd,
                KafkaConsumerFixtures.filledTopics(cmd),
                KafkaConsumerBenchmarks.consumerAtLeastOnceCommitEveryPoll())
  }

// These are not plotted anyway
//  it should "bench with normal messages" in {
//    val cmd = RunTestCommand(prefix + "consumer-normal-msg", bootstrapServers, topic_1000_5000)
//    runPerfTest(cmd,
//                KafkaConsumerFixtures.filledTopics(cmd),
//                KafkaConsumerBenchmarks.consumerAtLeastOnceCommitEveryPoll())
//  }
//
//  it should "bench with normal messages and eight partitions" in {
//    val cmd = RunTestCommand(prefix + "consumer-normal-msg-8-partitions",
//                             bootstrapServers,
//                             topic_1000_5000_8)
//    runPerfTest(cmd,
//                KafkaConsumerFixtures.filledTopics(cmd),
//                KafkaConsumerBenchmarks.consumerAtLeastOnceCommitEveryPoll())
//  }
}

class AlpakkaCommitAndForgetConsumer extends BenchmarksBase() {
  val prefix = "alpakka-kafka-commit-and-forget-"

  it should "bench with small messages" in {
    val cmd = RunTestCommand(prefix + "consumer", bootstrapServers, topic_1000_100)
    runPerfTest(cmd,
                ReactiveKafkaConsumerFixtures.committableSources(cmd),
                ReactiveKafkaConsumerBenchmarks.consumerCommitAndForget(commitBatchSize = 1000))
  }

// These are not plotted anyway
//  it should "bench with normal messages" in {
//    val cmd = RunTestCommand(prefix + "normal-msg", bootstrapServers, topic_1000_5000)
//    runPerfTest(cmd,
//                ReactiveKafkaConsumerFixtures.committableSources(cmd),
//                ReactiveKafkaConsumerBenchmarks.consumerCommitAndForget(commitBatchSize = 1000))
//  }
//
//  it should "bench with normal messages and eight partitions" in {
//    val cmd = RunTestCommand(prefix + "normal-msg-8-partitions",
//                             bootstrapServers,
//                             topic_1000_5000_8)
//    runPerfTest(cmd,
//                ReactiveKafkaConsumerFixtures.committableSources(cmd),
//                ReactiveKafkaConsumerBenchmarks.consumerCommitAndForget(commitBatchSize = 1000))
//  }
}
