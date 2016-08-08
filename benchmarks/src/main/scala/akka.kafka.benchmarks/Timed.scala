/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import java.util.concurrent.TimeUnit

import akka.kafka.benchmarks.app.RunTestCommand
import com.codahale.metrics.{Meter, MetricRegistry, ScheduledReporter, Slf4jReporter}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext

object Timed extends LazyLogging {

  implicit val ec = ExecutionContext.fromExecutor(new scala.concurrent.forkjoin.ForkJoinPool)

  def reporter(metricRegistry: MetricRegistry): ScheduledReporter = {
    Slf4jReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build()
  }

  def runPerfTest[F](command: RunTestCommand, fixtureGen: FixtureGen[F], testBody: (F, Meter) => Unit): Unit = {
    val name = command.testName
    val msgCount = command.msgCount
    logger.info(s"Generating fixture for ${name}_$msgCount")
    val fixture = fixtureGen.generate(msgCount)
    val metrics = new MetricRegistry()
    val meter = metrics.meter(name + "_" + msgCount)
    logger.info(s"Running benchmarks for ${name}_$msgCount")
    val now = System.currentTimeMillis()
    testBody(fixture, meter)
    val after = System.currentTimeMillis()
    logger.info(s"Test ${name}_$msgCount took ${after - now} ms")
    reporter(metrics).report()
  }
}
