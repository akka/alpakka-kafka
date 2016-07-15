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
import scala.language.postfixOps

class BenchmarksSpec extends TestKit(ActorSystem("AkkaKafkaBenchmarks")) with FlatSpecLike with BeforeAndAfterAll {

  implicit val mat = ActorMaterializer()

  it should "work" in {
    Benchmarks.run(RunTestCommand("all", "localhost:9092", 100000, 200000, 25000))
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }
}