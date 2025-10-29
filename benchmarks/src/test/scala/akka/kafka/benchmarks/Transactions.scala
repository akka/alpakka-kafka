/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.BenchmarksBase.{topic_100_100, topic_100_5000}
import akka.kafka.benchmarks.Timed.runPerfTest
import akka.kafka.benchmarks.app.RunTestCommand
import scala.concurrent.duration._

class ApacheKafkaTransactions extends BenchmarksBase() {
  it should "bench with small messages" in {
    val cmd = RunTestCommand("apache-kafka-transactions", bootstrapServers, topic_100_100)
    runPerfTest(cmd,
                KafkaTransactionFixtures.initialize(cmd),
                KafkaTransactionBenchmarks.consumeTransformProduceTransaction(commitInterval = 100.milliseconds))
  }

  it should "bench with normal messages" in {
    val cmd = RunTestCommand("apache-kafka-transactions-normal-msg", bootstrapServers, topic_100_5000)
    runPerfTest(cmd,
                KafkaTransactionFixtures.initialize(cmd),
                KafkaTransactionBenchmarks.consumeTransformProduceTransaction(commitInterval = 100.milliseconds))
  }
}

class AlpakkaKafkaTransactions extends BenchmarksBase() {
  it should "bench with small messages" in {
    val cmd = RunTestCommand("alpakka-kafka-transactions", bootstrapServers, topic_100_100)
    runPerfTest(
      cmd,
      ReactiveKafkaTransactionFixtures.transactionalSourceAndSink(cmd, commitInterval = 100.milliseconds),
      ReactiveKafkaTransactionBenchmarks.consumeTransformProduceTransaction
    )
  }

  it should "bench with normal messages" in {
    val cmd = RunTestCommand("alpakka-kafka-transactions-normal-msg", bootstrapServers, topic_100_5000)
    runPerfTest(
      cmd,
      ReactiveKafkaTransactionFixtures.transactionalSourceAndSink(cmd, commitInterval = 100.milliseconds),
      ReactiveKafkaTransactionBenchmarks.consumeTransformProduceTransaction
    )
  }
}
