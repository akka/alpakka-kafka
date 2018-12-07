/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.benchmarks.app.RunTestCommand
import akka.stream.Materializer
import Timed._
import scala.concurrent.Future
import scala.concurrent.duration._

object Benchmarks {

  def run(cmd: RunTestCommand)(implicit actorSystem: ActorSystem, mat: Materializer): Unit =
    cmd.testName match {
      case "apache-kafka-plain-consumer-nokafka" =>
        runPerfTest(cmd, KafkaConsumerFixtures.noopFixtureGen(cmd), KafkaConsumerBenchmarks.consumePlainNoKafka)
      case "alpakka-kafka-plain-consumer-nokafka" =>
        runPerfTest(cmd,
                    ReactiveKafkaConsumerFixtures.noopFixtureGen(cmd),
                    ReactiveKafkaConsumerBenchmarks.consumePlainNoKafka)
      case "apache-kafka-plain-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumePlain)
      case "alpakka-kafka-plain-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.plainSources(cmd), ReactiveKafkaConsumerBenchmarks.consumePlain)
      case "apache-kafka-batched-consumer" =>
        runPerfTest(cmd,
                    KafkaConsumerFixtures.filledTopics(cmd),
                    KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
      case "alpakka-kafka-batched-consumer" =>
        runPerfTest(cmd,
                    ReactiveKafkaConsumerFixtures.committableSources(cmd),
                    ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
      case "apache-kafka-at-most-once-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case "alpakka-kafka-at-most-once-consumer" =>
        runPerfTest(cmd,
                    ReactiveKafkaConsumerFixtures.committableSources(cmd),
                    ReactiveKafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case "apache-kafka-plain-producer" =>
        runPerfTest(cmd, KafkaProducerFixtures.initializedProducer(cmd), KafkaProducerBenchmarks.plainFlow)
      case "alpakka-kafka-plain-producer" =>
        runPerfTest(cmd, ReactiveKafkaProducerFixtures.flowFixture(cmd), ReactiveKafkaProducerBenchmarks.plainFlow)
      case "apache-kafka-transactions" =>
        runPerfTest(cmd,
                    KafkaTransactionFixtures.initialize(cmd),
                    KafkaTransactionBenchmarks.consumeTransformProduceTransaction(commitInterval = 100.milliseconds))
      case "alpakka-kafka-transactions" =>
        runPerfTest(
          cmd,
          ReactiveKafkaTransactionFixtures.transactionalSourceAndSink(cmd, commitInterval = 100.milliseconds),
          ReactiveKafkaTransactionBenchmarks.consumeTransformProduceTransaction
        )
      case _ => Future.failed(new IllegalArgumentException(s"Unrecognized test name: ${cmd.testName}"))
    }

}
