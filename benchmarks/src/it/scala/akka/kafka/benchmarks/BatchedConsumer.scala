package akka.kafka.benchmarks

import akka.kafka.benchmarks.BenchmarksBase.{topic_1000_100, topic_1000_5000, topic_1000_5000_8, topic_1000_5000_100}
import akka.kafka.benchmarks.Timed.runPerfTest
import akka.kafka.benchmarks.app.RunTestCommand

class ApacheKafkaBatchedConsumer extends BenchmarksBase() {
  it should "bench with small messages" in {
    val cmd = RunTestCommand("apache-kafka-batched-consumer", bootstrapServers, topic_1000_100.freshTopic)
    runPerfTest(cmd,
      KafkaConsumerFixtures.filledTopics(cmd),
      KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
  }

  it should "bench with normal messages" in {
    val cmd =
      RunTestCommand("apache-kafka-batched-consumer-normal-msg", bootstrapServers, topic_1000_5000.freshTopic)
    runPerfTest(cmd,
      KafkaConsumerFixtures.filledTopics(cmd),
      KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
  }

  it should "bench with normal messages and eight partitions" in {
    val cmd =
      RunTestCommand("apache-kafka-batched-consumer-normal-msg-8-partitions", bootstrapServers, topic_1000_5000_8.freshTopic)
    runPerfTest(cmd,
      KafkaConsumerFixtures.filledTopics(cmd),
      KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
  }
}

class AlpakkaKafkaBatchedConsumer extends BenchmarksBase() {

  it should "bench with small messages" in {
    val cmd = RunTestCommand("alpakka-kafka-batched-consumer", bootstrapServers, topic_1000_100)
    runPerfTest(cmd,
      ReactiveKafkaConsumerFixtures.committableSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
  }

  it should "bench with normal messages" in {
    val cmd = RunTestCommand("alpakka-kafka-batched-consumer-normal-msg", bootstrapServers, topic_1000_5000)
    runPerfTest(cmd,
      ReactiveKafkaConsumerFixtures.committableSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
  }

  it should "bench with normal messages and eight partitions" in {
    val cmd =
      RunTestCommand("alpakka-kafka-batched-consumer-normal-msg-8-partitions", bootstrapServers, topic_1000_5000_8)
    runPerfTest(cmd,
      ReactiveKafkaConsumerFixtures.committableSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
  }

  it should "bench with normal messages and one hundred partitions" in {
    val cmd =
      RunTestCommand("alpakka-kafka-batched-consumer-normal-msg-100-partitions", bootstrapServers, topic_1000_5000_100)
    runPerfTest(cmd,
      ReactiveKafkaConsumerFixtures.committableSources(cmd),
      ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
  }
}
