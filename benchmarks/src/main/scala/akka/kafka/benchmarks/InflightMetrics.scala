/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.lang.management.{BufferPoolMXBean, ManagementFactory, MemoryType}

import akka.NotUsed
import akka.actor.Cancellable
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import javax.management.{Attribute, MBeanServerConnection, ObjectName}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

private[benchmarks] trait InflightMetrics {
  import InflightMetrics._

  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
  private val memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans.asScala
  private val directBufferPoolBeans = ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala

  private val compatibleGcNames = List("ConcurrentMarkSweep", "PS MarkSweep")

  private val timeMsHeader = "time-ms"
  private val jvmHeaders =
    List("gc-count", "gc-time-ms", "heap-bytes", "non-heap-bytes", "direct-bytes").map("jvm:" + _)
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
  )(implicit mat: Materializer): Source[List[String], Cancellable] = {
    implicit val ec: ExecutionContext = mat.executionContext

    val consumerMetricNamesSorted = consumerMetricNames.sortBy(_.name)
    val brokerMetricNamesSorted = brokerMetricNames.sortBy(_.name)

    val accStart = (0.seconds, None.asInstanceOf[Option[List[Metric]]])
    val brokersJmx = jmxConnections(brokerJmxUrls)

    val metricsFetcher = Source
      .tick(0.seconds, interval, NotUsed)
      .scanAsync(accStart)({
        case ((timeMs, accLastMetrics), _) =>
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
              val newMetrics = Gauge(timeMs.toMillis.toDouble) +: (jvmMetrics ++ consumerMetrics ++ brokerMetrics)
              val nextInterval = interval + timeMs
              val nextAcc = accLastMetrics match {
                case None => Counter.reset(newMetrics)
                case Some(lastMetrics) => Counter.update(lastMetrics, newMetrics)
              }
              (nextInterval, Some(nextAcc))
            case _ => throw new IllegalStateException("The wrong number of Future results were returned.")
          }
      })
      .mapConcat { case (_, results: Option[List[Metric]]) => results.map(_.map(_.value.toString)).toList }

    val header = Source.single(
      timeMsHeader +: (
        jvmHeaders ++
        consumerMetricNamesSorted.map(consumerHeaderPrefix + _.name) ++
        brokerMetricNamesSorted.map(brokerHeaderPrefix + _.name)
      )
    )

    // prepend header row to stream and use `Cancellable` mat value from metricsFetcher stream
    header.concatMat(metricsFetcher) { case (_, metricsMat) => metricsMat }
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
  ): Future[List[Metric]] = {
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

      val results: List[Metric] = requests
        .zip(metricValues)
        .map {
          case (ConsumerMetricRequest(_, GaugeMetricType), value) => Gauge(value)
          case (ConsumerMetricRequest(_, CounterMetricType), value) => Counter(value)
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
  )(implicit ec: ExecutionContext): Future[List[Metric]] = Future {
    brokerMetricNames
      .sortBy(_.name)
      .map {
        case bmr @ BrokerMetricRequest(_, _, attrName, _) =>
          val sum = brokersJmx
            .map(conn => getRemoteJmxValue(bmr.topicMetricName, attrName, conn))
            .sum
          Counter(parseNumeric(sum))
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
  final case class ConsumerMetricResult(name: String, rawMetric: Object)

  final case class BrokerMetricRequest(name: String, topic: String, attribute: String, metricType: MetricType) {
    val topicMetricName: String = s"$name,topic=$topic"
  }

  final case class BrokerMetricResult(request: BrokerMetricRequest, metric: Metric)

  sealed trait Metric {
    def value: Double
    override def toString: String = value.toString
  }

  final case class Gauge(value: Double) extends Metric
  final case class Counter(value: Double) extends Metric
  private final case class BaseCounter(base: Double, newValue: Double) extends Metric {
    val value: Double = newValue - base
    def update(newValue: Double): BaseCounter = BaseCounter(base, newValue)
  }

  object Counter {
    def reset(metrics: List[Metric]): List[Metric] = metrics.map {
      case metric: Counter => BaseCounter(metric.value, metric.value)
      case metric => metric
    }

    def update(lastMetrics: List[Metric], newMetrics: List[Metric]): List[Metric] =
      newMetrics.zip(lastMetrics).map {
        case (newMetric: BaseCounter, lastMetric: BaseCounter) => lastMetric.update(newMetric.value)
        case (newMetric, _) => newMetric
      }
  }

  def parseNumeric(n: Any): Double = n match {
    case n: Double => n
    case n: Long => n.toDouble
    case o => java.lang.Double.parseDouble(o.toString)
  }
}
