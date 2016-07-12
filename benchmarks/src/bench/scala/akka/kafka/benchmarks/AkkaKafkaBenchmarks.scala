/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks
import akka.actor.ActorSystem
import akka.kafka.benchmarks.Timed._
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.language.postfixOps

class AkkaKafkaBenchmarks extends TestKit(ActorSystem("AkkaKafkaBenchmarks")) with FlatSpecLike with BeforeAndAfterAll {
  val zookeeperConnect = "localhost:2181"
  val kafkaHost = "localhost:9092"
  val minMessages = 1000000
  val maxMessages = 2000000
  val hop = 350000

  implicit val mat = ActorMaterializer()

  val kafkaConsumerFixtures = KafkaConsumerFixtures.filledTopics(kafkaHost, "msgCount")(minMessages, maxMessages, hop)
  val reactiveConsumerFixtures = ReactiveKafkaConsumerFixtures.filledTopics(kafkaHost, "msgCount")(minMessages, maxMessages, hop)

  it should "work" in {
    runPerfTest("plain consumer", kafkaConsumerFixtures, KafkaConsumerBenchmarks.consumePlain)
    runPerfTest("reactive consumer", reactiveConsumerFixtures, ReactiveKafkaConsumerBenchmarks.reactiveConsumePlain)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }
}