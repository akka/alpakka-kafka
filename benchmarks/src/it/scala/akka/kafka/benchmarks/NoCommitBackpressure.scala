package akka.kafka.benchmarks

import akka.kafka.benchmarks.Timed.runPerfTest
import akka.kafka.benchmarks.app.RunTestCommand

class RawKafkaCommitEveryPollConsumer extends BenchmarksBase() {
  private val prefix = "apache-kafka-batched-no-pausing-"

  it should "bench with small messages" in {
    val cmd = RunTestCommand(prefix + "consumer", bootstrapServers, 1000 * factor, 100)
    runPerfTest(cmd,
      KafkaConsumerFixtures.filledTopics(cmd),
      KafkaConsumerBenchmarks.consumerAtLeastOnceCommitEveryPoll())
  }

  it should "bench with normal messages" in {
    val cmd = RunTestCommand(prefix + "consumer-normal-msg", bootstrapServers, 1000 * factor, 5 * 1000)
    runPerfTest(cmd,
      KafkaConsumerFixtures.filledTopics(cmd),
      KafkaConsumerBenchmarks.consumerAtLeastOnceCommitEveryPoll())
  }

  it should "bench with normal messages and eight partitions" in {
    val cmd = RunTestCommand(prefix + "consumer-normal-msg-8-partitions",
      bootstrapServers,
      msgCount = 1000 * factor,
      msgSize = 5 * 1000,
      numberOfPartitions = 8)
    runPerfTest(cmd,
      KafkaConsumerFixtures.filledTopics(cmd),
      KafkaConsumerBenchmarks.consumerAtLeastOnceCommitEveryPoll())
  }
}

class AlpakkaCommitAndForgetConsumer extends BenchmarksBase() {
  val prefix = "alpakka-kafka-commit-and-forget-"

  it should "bench with small messages" in {
    val cmd = RunTestCommand(prefix + "consumer", bootstrapServers, 1000 * factor, 100)
    runPerfTest(cmd,
      ReactiveKafkaConsumerFixtures.committableSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumerCommitAndForget(commitBatchSize = 1000))
  }

  it should "bench with normal messages" in {
    val cmd = RunTestCommand(prefix + "normal-msg", bootstrapServers, 1000 * factor, 5 * 1000)
    runPerfTest(cmd,
      ReactiveKafkaConsumerFixtures.committableSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumerCommitAndForget(commitBatchSize = 1000))
  }

  it should "bench with normal messages and eight partitions" in {
    val cmd = RunTestCommand(prefix + "normal-msg-8-partitions",
      bootstrapServers,
      msgCount = 1000 * factor,
      msgSize = 5 * 1000,
      numberOfPartitions = 8)
    runPerfTest(cmd,
      ReactiveKafkaConsumerFixtures.committableSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumerCommitAndForget(commitBatchSize = 1000))
  }
}

