/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.benchmarks.Timed.runPerfTest
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.language.postfixOps

class AkkaKafkaBenchmarks extends TestKit(ActorSystem("AkkaKafkaBenchmarks")) with FlatSpecLike with BeforeAndAfterAll {
  val zookeeperConnect = "localhost:2181"
  val kafkaHost = "localhost:9092"

  implicit val mat = ActorMaterializer()

  val kafkaConsumerFixtures = KafkaConsumerFixtures.filledTopics(kafkaHost) _
  val reactiveConsumerFixtures = ReactiveKafkaConsumerFixtures.plainSources(kafkaHost) _
  val reactiveCommitableFixtures = ReactiveKafkaConsumerFixtures.commitableSources(kafkaHost) _

  it should "work" in {
    runPerfTest("plain-consumer", kafkaConsumerFixtures(10000000, 20000000, 3500000), KafkaConsumerBenchmarks.consumePlain)
    runPerfTest("akka-plain-consumer", reactiveConsumerFixtures(1000000, 2000000, 350000), ReactiveKafkaConsumerBenchmarks.consumePlain)

    runPerfTest("grouped-consumer", kafkaConsumerFixtures(5000000, 10000000, 2000000), KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 50000))
    runPerfTest("akka-grouped-within-consumer", reactiveCommitableFixtures(1000000, 2000000, 350000), ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 50000))

    runPerfTest("at-most-once-consumer", kafkaConsumerFixtures(50000, 100000, 20000), KafkaConsumerBenchmarks.consumeCommitAtMostOnce)
    runPerfTest("akka-at-most-once-consumer", reactiveCommitableFixtures(500, 1000, 200), ReactiveKafkaConsumerBenchmarks.consumeCommitAtMostOnce)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }
}