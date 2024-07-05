/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.lang.management.{BufferPoolMXBean, ManagementFactory, MemoryType}

import akka.NotUsed
import akka.actor.Cancellable
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.codahale.metrics.{Histogram, MetricRegistry}
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import javax.management.{Attribute, MBeanServerConnection, ObjectName}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

private[benchmarks] trait InflightMetrics {
  import InflightMetrics._

  private val registry = new MetricRegistry()

  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
  private val memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans.asScala
  private val directBufferPoolBeans = ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala

  private val compatibleGcNames = List("ConcurrentMarkSweep", "PS MarkSweep")

  private val timeMsHeader = "time-ms"
  private val (gcCountHeader, gcTimeMsHeader, heapBytesHeader, nonHeapBytesHeader, directBytesHeader) =
    ("gc-count", "gc-time-ms", "heap-bytes:mean", "non-heap-bytes:mean", "direct-bytes:mean")
  private val jvmHeaders =
    List(gcCountHeader, gcTimeMsHeader, heapBytesHeader, nonHeapBytesHeader, directBytesHeader).map("jvm:" + _)
  private val consumerHeaderPrefix = "kafka-consumer:"
  private val brokerHeaderPrefix = "broker:"

  /**
   * Every poll interval collect JVM and specified Kafka Consumer & Broker metrics.
   */
  def pollForMetrics(
      interval: FiniteDuration,
      control: Control,
      consumerMetricNames: List[ConsumerMetricRequest],
      brokerMetricNames: List[BrokerMetricRequest],
      brokerJmxUrls: List[String]
  )(implicit mat: Materializer): (Cancellable, Future[List[List[String]]]) = {
    implicit val ec: ExecutionContext = mat.executionContext

    val consumerMetricNamesSorted = consumerMetricNames.sortBy(_.name)
    val brokerMetricNamesSorted = brokerMetricNames.sortBy(_.name)

    val accStart = (0.seconds, None.asInstanceOf[Option[List[Metric]]])
    val brokersJmx = jmxConnections(brokerJmxUrls)

    val (metricsControl, metricsFuture) = Source
      .tick(0.seconds, interval, NotUsed)
      .scanAsync(accStart)({
        case ((timeMs, accLastMetrics), _) =>
          getAllMetrics(control, consumerMetricNamesSorted, brokerMetricNamesSorted, brokersJmx) map {
            case jvmMetrics :: consumerMetrics :: brokerMetrics :: Nil =>
              val timeMsMeasurement = Measurement(timeMsHeader, timeMs.toMillis.toDouble, GaugeMetricType)
              val newMetrics = timeMsMeasurement +: (jvmMetrics ++ consumerMetrics ++ brokerMetrics)
              val nextInterval = interval + timeMs
              val nextAcc = accLastMetrics match {
                case None => InflightMetrics.reset(newMetrics, registry)
                case Some(lastMetrics) => InflightMetrics.update(lastMetrics, newMetrics)
              }
              (nextInterval, Some(nextAcc))
            case _ => throw new IllegalStateException("The wrong number of Future results were returned.")
          }
      })
      .mapConcat { case (_, results: Option[List[Metric]]) => results.toList }
      .toMat(Sink.seq)(Keep.both)
      .run()

    val metricsWithHeaderAndFooter: Future[List[List[String]]] = metricsFuture.map { metrics =>
      val header = timeMsHeader +: metricHeaders(consumerMetricNamesSorted, brokerMetricNamesSorted)
      val metricsStrings = metrics.map(_.map(_.value.toString)).toList
      val summaryLine = metrics.last.map {
        case hg: HistogramGauge => hg.summaryValue.toString
        case metric => metric.value.toString
      }
      header +: metricsStrings :+ summaryLine
    }

    (metricsControl, metricsWithHeaderAndFooter)
  }

  private def metricHeaders(consumerMetricNamesSorted: List[ConsumerMetricRequest],
                            brokerMetricNamesSorted: List[BrokerMetricRequest]): List[String] = {
    jvmHeaders ++
    consumerMetricNamesSorted.map(consumerHeaderPrefix + _.name) ++
    brokerMetricNamesSorted.map(brokerHeaderPrefix + _.name.replace(",", ":"))
  }

  /**
   * Asynchronously retrieve all metrics
   */
  private def getAllMetrics(control: Control,
                            consumerMetricNamesSorted: List[ConsumerMetricRequest],
                            brokerMetricNamesSorted: List[BrokerMetricRequest],
                            brokersJmx: List[MBeanServerConnection])(implicit ec: ExecutionContext) = {
    Future.sequence(
      List(
        jvm(),
        consumer(control, consumerMetricNamesSorted),
        broker(brokersJmx, brokerMetricNamesSorted)
      )
    )
  }

  /**
   * Get all JVM metrics
   */
  private def jvm()(implicit ec: ExecutionContext) = Future {
    val (gcCount, gcTimeMs) = gc()
    val (heapBytes, nonHeapBytes, directBytes) = memoryUsage()
    val getMeanSummary: Option[com.codahale.metrics.Sampling => Long] = Some(_.getSnapshot.getMean.toLong)
    List(
      Measurement(gcCountHeader, gcCount, CounterMetricType),
      Measurement(gcTimeMsHeader, gcTimeMs, CounterMetricType),
      Measurement(heapBytesHeader, heapBytes, GaugeMetricType, getMeanSummary),
      Measurement(nonHeapBytesHeader, nonHeapBytes, GaugeMetricType, getMeanSummary),
      Measurement(directBytesHeader, directBytes, GaugeMetricType, getMeanSummary)
    )
  }

  /**
   * Use first GC pool that matches compatible GC names and return total GC count and last collection time length in ms.
   */
  private def gc() = {
    gcBeans
      .find(bean => compatibleGcNames.contains(bean.getName))
      .map(bean => (bean.getCollectionCount.toDouble, bean.getCollectionTime.toDouble))
      .getOrElse(
        throw new Exception(
          s"Compatible GC not found. Need one of: ${compatibleGcNames.mkString(",")}. Found ${gcBeans.map(_.getName()).mkString(",")}."
        )
      )
  }

  /**
   * Return JVM memory usage for on heap, JVM-managed off heap (code cache, metaspace, compressed class space), and
   * direct memory usages by end-users and NIO threadlocal BufferCache.
   */
  private def memoryUsage() = {
    val heapBytes = memoryPoolBeans.filter(_.getType == MemoryType.HEAP).map(_.getUsage.getUsed).sum.toDouble
    val nonHeapBytes = memoryPoolBeans.filter(_.getType == MemoryType.NON_HEAP).map(_.getUsage.getUsed).sum.toDouble
    val directBytes = directBufferPoolBeans.map(_.getMemoryUsed).sum.toDouble
    (heapBytes, nonHeapBytes, directBytes)
  }

  /**
   * Return specified consumer-level metrics using Alpakka Kafka's [[Control]] metrics API.
   */
  private def consumer[T](control: Control, requests: List[ConsumerMetricRequest])(
      implicit ec: ExecutionContext
  ): Future[List[Measurement]] = {
    control.metrics.map { consumerMetrics =>
      val metricValues = consumerMetrics
        .filter { case (name, _) => requests.map(_.name).contains(name.name()) }
        .filterNot { case (name, _) => name.tags.containsKey("topic") || name.tags.containsKey("partition") }
        .toList
        .sortBy { case (name, _) => name.name() }
        .map { case (_, value) => value.metricValue().asInstanceOf[Any] }
        .map(parseNumeric)

      require(metricValues.size == requests.size,
              "Number of returned metric values DNE number of requested consumer metrics")

      val results: List[Measurement] = requests
        .zip(metricValues)
        .map {
          case (ConsumerMetricRequest(name, metricType), value) => Measurement(name, value, metricType)
        }

      results
    }
  }

  /**
   * Return specified broker metrics using a remote JMX connection.
   */
  private def broker(
      brokersJmx: Seq[MBeanServerConnection],
      brokerMetricNames: List[BrokerMetricRequest]
  )(implicit ec: ExecutionContext): Future[List[Measurement]] = Future {
    brokerMetricNames
      .sortBy(_.name)
      .map {
        case bmr @ BrokerMetricRequest(name, _, attrName, _) =>
          val sum = brokersJmx
            .map(conn => getRemoteJmxValue(bmr.topicMetricName, attrName, conn))
            .sum
          Measurement(name, parseNumeric(sum), CounterMetricType)
      }
  }

  private def jmxConnections(brokerJmxUrls: List[String]): List[MBeanServerConnection] = {
    brokerJmxUrls.map { url =>
      val jmxUrl = new JMXServiceURL(url)
      val conn = JMXConnectorFactory.connect(jmxUrl)
      conn.getMBeanServerConnection
    }
  }

  private def getRemoteJmxValue(objectName: String, attrName: String, conn: MBeanServerConnection): Double = {
    val bytesInPerSec: ObjectName = new ObjectName(objectName)
    val attributes = Array(attrName)
    // to list all available objects and attributes you can: `conn.queryNames(null, null)`
    val attributeValues = conn.getAttributes(bytesInPerSec, attributes)
    val attr = attributeValues.stream.findFirst.get.asInstanceOf[Attribute]
    parseNumeric(attr.getValue)
  }
}

private[benchmarks] object InflightMetrics {
  sealed trait MetricType

  case object GaugeMetricType extends MetricType
  case object CounterMetricType extends MetricType

  final case class ConsumerMetricRequest(name: String, metricType: MetricType)
  final case class BrokerMetricRequest(name: String, topic: String, attribute: String, metricType: MetricType) {
    val topicMetricName: String = s"$name,topic=$topic"
  }

  /**
   * Optional summary value HoF lets you choose how to calculate the summary value of the gauge.  Choosing [[None]]
   * will return the last gauge value.  Providing a HoF allows you to extract a statistical calculation from a
   * dropwizard [[Sampling]].
   */
  final case class Measurement(name: String,
                               value: Double,
                               metricType: MetricType,
                               summaryValueF: Option[com.codahale.metrics.Sampling => Long] = None)

  sealed trait Metric {
    def measurement: Measurement
    def value: Double = measurement.value
    def update(measurement: Measurement): Metric
    override def toString: String = measurement.value.toString
  }

  private final case class HistogramGauge(measurement: Measurement, histogram: Histogram) extends Metric {
    histogram.update(measurement.value.toLong)
    def summaryValue: Double = measurement.summaryValueF.map(f => f(histogram).toDouble).getOrElse(value)
    def update(measurement: Measurement): HistogramGauge = HistogramGauge(measurement, histogram)
  }

  private final case class BaseCounter(base: Measurement, measurement: Measurement) extends Metric {
    override val value: Double = measurement.value - base.value
    def update(measurement: Measurement): Metric = BaseCounter(base, measurement)
  }

  def reset(measurements: List[Measurement], registry: MetricRegistry): List[Metric] = measurements.map {
    case measurement @ Measurement(_, _, CounterMetricType, _) => BaseCounter(measurement, measurement)
    case measurement @ Measurement(_, _, GaugeMetricType, _) =>
      HistogramGauge(measurement, registry.histogram(measurement.name))
  }

  def update(lastMetrics: List[Metric], measurements: List[Measurement]): List[Metric] =
    measurements.zip(lastMetrics).map {
      case (measurement, lastMetric) => lastMetric.update(measurement)
    }

  def parseNumeric(n: Any): Double = n match {
    case n: Double => n
    case n: Long => n.toDouble
    case o => java.lang.Double.parseDouble(o.toString)
  }
}
