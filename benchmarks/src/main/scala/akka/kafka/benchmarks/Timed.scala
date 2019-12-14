/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.nio.file.{Path, Paths}
import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.kafka.benchmarks.app.RunTestCommand
import com.codahale.metrics._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Timed extends LazyLogging {
  private val benchmarkReportBasePath = Paths.get("benchmarks", "target")

  implicit val ec = ExecutionContext.fromExecutor(new ForkJoinPool)

  def reporter(metricRegistry: MetricRegistry): ScheduledReporter =
    Slf4jReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build()

  def csvReporter(metricRegistry: MetricRegistry): ScheduledReporter =
    CsvReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(benchmarkReportBasePath.toFile)

  def runPerfTest[F](command: RunTestCommand, fixtureGen: FixtureGen[F], testBody: (F, Meter) => Unit): Unit = {
    val name = command.testName
    val msgCount = command.msgCount
    logger.info(s"Generating fixture for $name ${command.filledTopic}")
    val fixture = fixtureGen.generate(msgCount)
    val metrics = new MetricRegistry()
    val meter = metrics.meter(name)
    logger.info(s"Running benchmarks for $name")
    val now = System.nanoTime()
    testBody(fixture, meter)
    val after = System.nanoTime()
    val took = (after - now).nanos
    logger.info(s"Test $name took ${took.toMillis} ms")
    reporter(metrics).report()
    csvReporter(metrics).report()
  }

  def runPerfTestInflightMetrics[F](command: RunTestCommand,
                                    consumerMetricNames: List[String],
                                    fixtureGen: FixtureGen[F],
                                    testBody: (F, Meter, List[String], Path) => Unit): Unit = {
    val name = command.testName
    val msgCount = command.msgCount
    val metricsReportPath = benchmarkReportBasePath.resolve(Paths.get(s"$name-inflight-metrics.csv"))
    logger.info(s"Generating fixture for $name ${command.filledTopic}")
    val fixture = fixtureGen.generate(msgCount)
    val metrics = new MetricRegistry()
    val meter = metrics.meter(name)
    logger.info(s"Running benchmarks for $name")
    val now = System.nanoTime()
    testBody(fixture, meter, consumerMetricNames, metricsReportPath)
    val after = System.nanoTime()
    val took = (after - now).nanos
    logger.info(s"Test $name took ${took.toMillis} ms")
    reporter(metrics).report()
    csvReporter(metrics).report()
  }
}
