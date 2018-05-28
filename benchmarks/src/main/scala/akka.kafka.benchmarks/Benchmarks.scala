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

  def run(cmd: RunTestCommand)(implicit actorSystem: ActorSystem, mat: Materializer): Unit = {

    cmd.testName match {
      case "plain-consumer-nokafka" =>
        runPerfTest(cmd, KafkaConsumerFixtures.noopFixtureGen(cmd), KafkaConsumerBenchmarks.consumePlainNoKafka)
      case "akka-plain-consumer-nokafka" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.noopFixtureGen(cmd), ReactiveKafkaConsumerBenchmarks.consumePlainNoKafka)
      case "plain-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumePlain)
      case "akka-plain-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.plainSources(cmd), ReactiveKafkaConsumerBenchmarks.consumePlain)
      case "batched-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
      case "akka-batched-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.commitableSources(cmd), ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
      case "at-most-once-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case "akka-at-most-once-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.commitableSources(cmd), ReactiveKafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case "plain-producer" =>
        runPerfTest(cmd, KafkaProducerFixtures.initializedProducer(cmd), KafkaProducerBenchmarks.plainFlow)
      case "akka-plain-producer" =>
        runPerfTest(cmd, ReactiveKafkaProducerFixtures.flowFixture(cmd), ReactiveKafkaProducerBenchmarks.plainFlow)
      case "transactions" =>
        runPerfTest(cmd, KafkaTransactionFixtures.initialize(cmd), KafkaTransactionBenchmarks.consumeTransformProduceTransaction(commitInterval = 100.milliseconds))
      case "akka-transactions" =>
        runPerfTest(cmd, ReactiveKafkaTransactionFixtures.transactionalSourceAndSink(cmd, commitInterval = 100.milliseconds), ReactiveKafkaTransactionBenchmarks.consumeTransformProduceTransaction)
      case _ => Future.failed(new IllegalArgumentException(s"Unrecognized test name: ${cmd.testName}"))
    }
  }

}
