/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.nio.file.Paths
import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.kafka.benchmarks.InflightMetrics.{BrokerMetricRequest, ConsumerMetricRequest}
import akka.kafka.benchmarks.app.RunTestCommand
import akka.stream.Materializer
import akka.stream.alpakka.csv.scaladsl.CsvFormatting
import akka.stream.scaladsl.{FileIO, Sink, Source}
import com.codahale.metrics._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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

  def inflightMetricsReport(inflight: List[List[String]], testName: String)(
      implicit mat: Materializer
  ) = {
    val metricsReportPath = benchmarkReportBasePath.resolve(Paths.get(s"$testName-inflight-metrics.csv"))
    val metricsReportDetailPath = benchmarkReportBasePath.resolve(Paths.get(s"$testName-inflight-metrics-details.csv"))
    require(inflight.size > 1, "At least 2 records (a header and a data row) are required to make a report.")
    val summary = Source(List(inflight.head, inflight.last))
      .via(CsvFormatting.format())
      .alsoTo(Sink.foreach(bs => logger.info(bs.utf8String)))
      .runWith(FileIO.toPath(metricsReportPath))
    val details = Source(inflight).via(CsvFormatting.format()).runWith(FileIO.toPath(metricsReportDetailPath))
    implicit val ec: ExecutionContext = mat.executionContext
    Await.result(Future.sequence(List(summary, details)), 10.seconds)
  }

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

  def runPerfTestInflightMetrics[F](
      command: RunTestCommand,
      consumerMetricNames: List[ConsumerMetricRequest],
      brokerMetricNames: List[BrokerMetricRequest],
      brokerJmxUrls: List[String],
      fixtureGen: FixtureGen[F],
      testBody: (F, Meter, List[ConsumerMetricRequest], List[BrokerMetricRequest], List[String]) => List[List[String]]
  )(implicit mat: Materializer): Unit = {
    val name = command.testName
    val msgCount = command.msgCount
    logger.info(s"Generating fixture for $name ${command.filledTopic}")
    val fixture = fixtureGen.generate(msgCount)
    val metrics = new MetricRegistry()
    val meter = metrics.meter(name)
    logger.info(s"Running benchmarks for $name")
    val now = System.nanoTime()
    val inflight = testBody(fixture, meter, consumerMetricNames, brokerMetricNames, brokerJmxUrls)
    val after = System.nanoTime()
    val took = (after - now).nanos
    logger.info(s"Test $name took ${took.toMillis} ms")
    inflightMetricsReport(inflight, name)
    reporter(metrics).report()
    csvReporter(metrics).report()
  }
}
