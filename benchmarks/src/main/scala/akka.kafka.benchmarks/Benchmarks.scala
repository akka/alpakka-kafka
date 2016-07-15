/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.benchmarks.app.RunTestCommand
import akka.stream.{Materializer, ActorMaterializer}
import Timed._
import scala.concurrent.Future
import scala.language.postfixOps

object Benchmarks {

  def run(cmd: RunTestCommand)(implicit actorSystem: ActorSystem, mat: Materializer): Future[Unit] = {

    cmd.testName match {
      case "plain-consumer-nokafka" =>
        runPerfTest(cmd, KafkaConsumerFixtures.noopFixtureGen(cmd), KafkaConsumerBenchmarks.consumePlainNoKafka)
      case "akka-plain-consumer-nokafka" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.noopFixtureGen(cmd), ReactiveKafkaConsumerBenchmarks.consumePlainNoKafka)
      case "plain-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumePlain)
      case "akka-plain-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.plainSources(cmd), ReactiveKafkaConsumerBenchmarks.consumePlain)
      case "grouped-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 50000))
      case "akka-grouped-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.commitableSources(cmd), ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 50000))
      case "at-most-once-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case "akka-at-most-once-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.commitableSources(cmd), ReactiveKafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case _ => Future.failed(new IllegalArgumentException(s"Unrecognized test name: ${cmd.testName}"))

    }
  }

}
