/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.benchmarks.app.RunTestCommand
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class BenchmarksSpec extends TestKit(ActorSystem("AkkaKafkaBenchmarks")) with FlatSpecLike with BeforeAndAfterAll {

  val kafkaHost = "localhost:9092"

  implicit val mat = ActorMaterializer()

  it should "work" in {
    Benchmarks.run(RunTestCommand("akka-batched-consumer", kafkaHost, 2000000, 2000000, 0))
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }
}