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
  val timeout: FiniteDuration = 1 hour

  implicit val mat = ActorMaterializer()

  it should "work" in {
    Await.result(
    for {
      _ <- Benchmarks.run(RunTestCommand("plain-consumer", kafkaHost, 10000000, 20000000, 2500000))
      _ <- Benchmarks.run(RunTestCommand("akka-plain-consumer", kafkaHost, 10000000, 20000000, 2500000))
    } yield ()
    , timeout)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }
}