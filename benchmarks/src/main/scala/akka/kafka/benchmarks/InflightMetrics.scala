/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.lang.management.{BufferPoolMXBean, ManagementFactory, MemoryType}

import akka.NotUsed
import akka.actor.Cancellable
import akka.kafka.benchmarks.InflightMetrics._
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.alpakka.csv.scaladsl.CsvFormatting
import akka.stream.scaladsl.Source
import akka.util.ByteString
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import javax.management.{Attribute, MBeanServerConnection, ObjectName}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

private[benchmarks] trait InflightMetrics {
  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
  private val memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans.asScala
  private val directBufferPoolBeans = ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala

  private val compatibleGcNames = List("ConcurrentMarkSweep", "PS MarkSweep")
  private val baseHeader = List("time-ms", "gc-count", "gc-time-ms", "heap-bytes", "non-heap-bytes", "direct-bytes")

  /**
   * Every poll interval collect JVM GC, JVM Memory, and specified Kafka Consumer metrics and transform them into a CSV
   * formatted output stream.
   */
  def pollForMetrics(
      interval: FiniteDuration,
      control: Control,
      consumerMetricNames: List[ConsumerMetricRequest],
      brokerMetricNames: List[BrokerMetricRequest],
      brokerJmxUrls: List[String]
  )(implicit mat: Materializer): Source[ByteString, Cancellable] = {
    implicit val ec: ExecutionContext = mat.executionContext

    val consumerMetricNamesSorted = consumerMetricNames.sortBy(_.name)
    val brokerMetricNamesSorted = brokerMetricNames.sortBy(_.name)

    val accStart = (0.seconds, None.asInstanceOf[Option[List[Metric]]])
    val brokersJmx: Seq[MBeanServerConnection] = brokerJmxUrls.map { url =>
      val jmxUrl = new JMXServiceURL(url)
      val conn = JMXConnectorFactory.connect(jmxUrl)
      conn.getMBeanServerConnection
    }

    val metricsFetcher = Source
      .tick(0.seconds, interval, NotUsed)
      .scanAsync(accStart)({
        // base case
        //case ((timeMs, None), _) =>
        case ((timeMs, lastMetricsAcc), _) =>
          val (gcCount, gcTimeMs) = gc()
          val (heapBytes, nonHeapBytes, directBytes) = memoryUsage()
          val jvmMetrics =
            List(Counter(gcCount), Counter(gcTimeMs), Gauge(heapBytes), Gauge(nonHeapBytes), Gauge(directBytes))

          val asyncMetrics = List(
            consumer(control, consumerMetricNamesSorted),
            broker(brokersJmx, brokerMetricNamesSorted)
          )

          Future.sequence(asyncMetrics) map {
            case consumerMetrics :: brokerMetrics :: Nil =>
              val newMetrics = Gauge(timeMs.toMillis) +: (jvmMetrics ++ consumerMetrics ++ brokerMetrics)

              val nextInterval = interval + timeMs
              val nextAcc = lastMetricsAcc match {
                case Some(lastMetrics) => updateCounters(lastMetrics, newMetrics)
                case None =>
                  val metrics = Gauge(timeMs.toMillis) +: (jvmMetrics ++ consumerMetrics ++ brokerMetrics)
                  initCounters(metrics)
              }

              (nextInterval, Some(nextAcc))

            case _ => throw new IllegalStateException("The wrong number of Future results were returned.")
          }
      })
      .mapConcat { case (_, results: Option[List[Metric]]) => results.map(_.map(_.value.toString)).toList }

    val header = Source.single(
      baseHeader ++ consumerMetricNamesSorted.map("kafka-consumer:" + _) ++ brokerMetricNamesSorted.map("broker:" + _)
    )

    // prepend header row to stream and use `Cancellable` mat value from metricsFetcher stream
    header
      .concatMat(metricsFetcher) { case (_, metricsMat) => metricsMat }
      .via(CsvFormatting.format())
  }

  private def initCounters(metrics: List[Metric]) = metrics.map {
    case metric: Counter => BaseCounter.init(metric.value)
    case metric => metric
  }

  private def updateCounters(lastMetrics: List[InflightMetrics.Metric],
                             newMetrics: List[InflightMetrics.Metric]): List[InflightMetrics.Metric] =
    newMetrics.zip(lastMetrics).map {
      case (newMetric: BaseCounter, oldMetric: BaseCounter) => oldMetric.update(newMetric.value)
      case (newMetric, _) => newMetric
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
  ): Future[List[InflightMetrics.Metric]] = {
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

      val results: List[InflightMetrics.Metric] = requests
        .zip(metricValues)
        .map {
          case (_: GaugeConsumerMetricRequest, value) => Gauge(value)
          case (_: BaseCounterConsumerMetricRequest, value) => Counter(value)
        }

      results
    }
  }

  private def broker(
      brokersJmx: Seq[MBeanServerConnection],
      brokerMetricNames: List[BrokerMetricRequest]
  )(implicit ec: ExecutionContext): Future[List[InflightMetrics.Metric]] = Future {
    brokerMetricNames
      .sortBy(_.name)
      .map {
        case BaseCountBrokerMetricRequest(objectName, attrName) =>
          val sum = brokersJmx
            .map(conn => getRemoteJmxValue(objectName, attrName, conn))
            .sum
          Counter(parseNumeric(sum))
      }
  }

  private def getRemoteJmxValue(objectName: String, attrName: String, conn: MBeanServerConnection): Double = {
    val bytesInPerSec: ObjectName = new ObjectName(objectName)
    val attributes = Array(attrName)
    val attributeValues = conn.getAttributes(bytesInPerSec, attributes)
    val attr = attributeValues.stream.findFirst.get.asInstanceOf[Attribute]
    parseNumeric(attr.getValue())
  }

  private def parseNumeric(n: Any): Double = n match {
    case n: Double => n
    case n: Long => n.toDouble
    case o => java.lang.Double.parseDouble(o.toString)
  }
}

private[benchmarks] object InflightMetrics {
  sealed trait ConsumerMetricRequest {
    def name: String
  }

  final case class GaugeConsumerMetricRequest(name: String) extends ConsumerMetricRequest
  final case class BaseCounterConsumerMetricRequest(name: String) extends ConsumerMetricRequest

  // TODO: repurpose to clean up pattern matching
  //final case class ConsumerMetricResult(request: ConsumerMetricRequest, metric: Metric)

  sealed trait BrokerMetricRequest {
    def name: String
    def attribute: String
  }

  final case class BaseCountBrokerMetricRequest(name: String, attribute: String) extends BrokerMetricRequest

  //final case class BrokerMetricResult(request: BrokerMetricRequest, metric: Metric)

  final case class MetricResults(timeMs: Long,
                                 jvmMetrics: List[Metric],
                                 consumerMetrics: List[Metric],
                                 brokerMetrics: List[Metric]) {
    val toList: List[String] =
      timeMs.toString +: (jvmMetrics.map(_.value.toString) ++ consumerMetrics.map(_.value.toString) ++ brokerMetrics
        .map(_.value.toString))
  }

  sealed trait Metric {
    def value: Double
    override def toString: String = value.toString
  }

  sealed trait MetricType

  final case class Gauge(value: Double) extends Metric
  final case class Counter(value: Double) extends Metric

  final case class BaseCounter(base: Double, newValue: Double) extends Metric {
    val value: Double = newValue - base
    def update(newValue: Double): BaseCounter = BaseCounter(base, newValue)
  }

  object BaseCounter {
    def init(newValue: Double): BaseCounter = BaseCounter(newValue, newValue)
  }
}
