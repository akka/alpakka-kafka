package akka.kafka.benchmarks

import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import com.typesafe.scalalogging.LazyLogging

object Timed extends LazyLogging {

  def reporter(metricRegistry: MetricRegistry): ScheduledReporter = {
    Slf4jReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build()
  }

  def runPerfTest[F](name: String, fixtureGen: FixtureGen[F], testBody: (F, Meter) => Unit): Unit = {
    val warmupRegistry = new MetricRegistry()
    logger.info("Warming up")
    val warmupMeter = warmupRegistry.meter(name + "_" + "-ignore-warmup")
    fixtureGen.warmupset.foreach { fixture =>
      testBody(fixture, warmupMeter)
    }
    logger.info("Warmup complete")
    fixtureGen.dataset.foreach { msgCount =>
      logger.info(s"Generating fixture for ${name}_$msgCount")
      val fixture = fixtureGen.generate(msgCount)
      val metrics = new MetricRegistry()
      val meter = metrics.meter(name + "_" + msgCount)
      logger.info(s"Running benchmarks for ${name}_$msgCount")
      testBody(fixture, meter)
      reporter(metrics).report()
    }
  }
}