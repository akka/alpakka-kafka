package akka.kafka.benchmarks

import akka.kafka.benchmarks.BenchmarksBase.{topic_100_100, topic_100_5000}
import akka.kafka.benchmarks.Timed.runPerfTest
import akka.kafka.benchmarks.app.RunTestCommand

/**
 * Compares the `CommittingProducerSinkStage` with the composed implementation of `Producer.flexiFlow` and `Committer.sink`.
 */
class AlpakkaCommittableProducer extends BenchmarksBase() {
  it should "bench composed sink with 100b messages" in {
    val cmd = RunTestCommand("alpakka-committable-producer-composed", bootstrapServers, topic_100_100)
    runPerfTest(
      cmd,
      AlpakkaCommittableSinkFixtures.composedSink(cmd),
      AlpakkaCommittableSinkBenchmarks.run
    )
  }

  it should "bench composed sink with 5000b messages" in {
    val cmd = RunTestCommand("alpakka-committable-producer-composed-5000b", bootstrapServers, topic_100_5000)
    runPerfTest(
      cmd,
      AlpakkaCommittableSinkFixtures.composedSink(cmd),
      AlpakkaCommittableSinkBenchmarks.run
    )
  }

  it should "bench `Producer.committableSink` with 100b messages" in {
    val cmd = RunTestCommand("alpakka-committable-producer", bootstrapServers, topic_100_100)
    runPerfTest(
      cmd,
      AlpakkaCommittableSinkFixtures.producerSink(cmd),
      AlpakkaCommittableSinkBenchmarks.run
    )
  }

  it should "bench `Producer.committableSink` with 5000b messages" in {
    val cmd = RunTestCommand("alpakka-committable-producer-5000b", bootstrapServers, topic_100_5000)
    runPerfTest(
      cmd,
      AlpakkaCommittableSinkFixtures.producerSink(cmd),
      AlpakkaCommittableSinkBenchmarks.run
    )
  }
}
