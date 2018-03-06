/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks.app

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.benchmarks.Benchmarks
import akka.stream.ActorMaterializer
import scala.concurrent.ExecutionContext

trait BaseComponent {
  protected implicit def log: LoggingAdapter
  protected implicit def executor: ExecutionContext
}

object System {
  implicit val system = ActorSystem("akka-kafka-benchmarks")
  implicit val materializer = ActorMaterializer()

  trait LoggerExecutor extends BaseComponent {
    protected implicit val executor = system.dispatcher
    protected implicit val log = Logging(system, "app")
  }
}

object BenchmarksApp extends App with Config with System.LoggerExecutor {

  import System._

  log.info("App started")
  Benchmarks.run(RunTestCommand(testName, kafkaHost, msgCount))
}
