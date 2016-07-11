/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.actor.ActorSystem
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object AkkaKafkaBenchmarks extends App {

  implicit val system = ActorSystem("akka-kafka-benchmarks")

  // TODO run actual benchmarks here
  println("Sleeping...")
  Thread.sleep(2.seconds.toMillis)
  println("Application exiting")
  Await.result(system.terminate(), atMost = 10 seconds)
}
