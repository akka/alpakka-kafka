/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks.app

import akka.http.scaladsl.server.Directives._
import akka.kafka.benchmarks.Benchmarks

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait BenchmarksWebService extends BaseService {

  import System._

  protected val routes = pathPrefix("test") {
    get {
      parameters('testName, 'from.as[Int] ? 100000, 'upto.as[Int] ? 200000, 'hop.as[Int] ? 25000) {
        (testName, from, upto, hop) =>
          Future {
            Benchmarks.run(RunTestCommand(testName, kafkaHost, from, upto, hop))
          }
            .onComplete {
              case Success(_) => log.info(s"Test $testName finished")
              case Failure(ex) => log.error(ex, s"Test $testName failed")
            }
          complete(s"Started $testName with kafka host $kafkaHost. Starting with $from messages, increasing by $hop up to $upto")
      }
    }
  } ~ pathPrefix("status") {
    get {
      complete("Service available.")
    }
  }
}
