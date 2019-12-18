/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.lang.management.{BufferPoolMXBean, ManagementFactory, MemoryType}

import akka.NotUsed
import akka.actor.Cancellable
import akka.kafka.benchmarks.InflightMetrics.{BrokerMetricRequest, BrokerMetricResult}
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.alpakka.csv.scaladsl.CsvFormatting
import akka.stream.scaladsl.Source
import akka.util.ByteString
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import javax.management.{Attribute, MBeanServerConnection, ObjectName}
import org.apache.kafka.common.{Metric, MetricName}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

private[benchmarks] trait InflightMetrics {
  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
  private val memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans.asScala
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
      consumerMetricNames: List[String],
      brokerMetricNames: List[BrokerMetricRequest],
      brokerJmxUrls: List[String]
  )(implicit mat: Materializer): Source[ByteString, Cancellable] = {
    implicit val ec: ExecutionContext = mat.executionContext

    val consumerMetricNamesSorted: List[String] = consumerMetricNames.sorted
    val brokerMetricNamesSorted: List[String] = brokerMetricNames.map(_.name).sorted

    val accStart = (0.seconds, None.asInstanceOf[Option[List[String]]])
    val brokersJmx: Seq[MBeanServerConnection] = brokerJmxUrls.map { url =>
      val jmxUrl = new JMXServiceURL(url)
      val conn = JMXConnectorFactory.connect(jmxUrl)
      conn.getMBeanServerConnection
    }

    val metricsFetcher = Source
      .tick(0.seconds, interval, NotUsed)
      .scanAsync(accStart)({
        case ((timeMs, _), _) =>
          val (gcCount, gcTimeMs) = gc()
          val (heapBytes, nonHeapBytes, directBytes) = memoryUsage()
          val baseRow = List(timeMs.toMillis.toString, gcCount, gcTimeMs, heapBytes, nonHeapBytes, directBytes)

          val asyncMetrics = List(
            consumer(control, consumerMetricNamesSorted),
            broker(brokersJmx, brokerMetricNames)
          )

          Future.sequence(asyncMetrics) map {
            case consumerMetrics :: brokerMetrics :: Nil =>
              (interval + timeMs, Some(baseRow ++ consumerMetrics ++ brokerMetrics))
            case _ => throw new IllegalStateException("The wrong number of Future results were returned.")
          }
      })
      .mapConcat { case (_, o: Option[List[String]]) => o.toList }

    val header = Source.single(
      baseHeader ++ consumerMetricNamesSorted.map("kafka-consumer:" + _) ++ brokerMetricNamesSorted.map("broker:" + _)
    )

    // prepend header row to stream and use `Cancellable` mat value from metricsFetcher stream
    header
      .concatMat(metricsFetcher) { case (_, metricsMat) => metricsMat }
      .via(CsvFormatting.format())
  }

  /**
   * Use first GC pool that matches compatible GC names and return total GC count and last collection time length in ms.
   */
  private def gc(): (String, String) = {
    gcBeans
      .find(bean => compatibleGcNames.contains(bean.getName))
      .map(bean => (bean.getCollectionCount.toString, bean.getCollectionTime.toString))
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
  private def memoryUsage(): (String, String, String) = {
    val heapBytes = memoryPoolMXBeans.filter(_.getType == MemoryType.HEAP).map(_.getUsage.getUsed).sum
    val nonHeapBytes = memoryPoolMXBeans.filter(_.getType == MemoryType.NON_HEAP).map(_.getUsage.getUsed).sum
    val directBytes = directBufferPoolBeans.map(_.getMemoryUsed).sum
    (heapBytes.toString, nonHeapBytes.toString, directBytes.toString)
  }

  /**
   * Return specified consumer-level metrics using Alpakka Kafka's [[Control]] metrics API.
   */
  private def consumer(control: Control,
                       consumerMetricNamesSorted: List[String])(implicit ec: ExecutionContext): Future[List[String]] = {
    control.metrics.map { consumerMetrics =>
      val metrics = consumerMetrics
        .map { case (name, metric) => InflightMetrics.ConsumerMetricResult(name, metric) }
        .filter(cm => consumerMetricNamesSorted.contains(cm.name.name()))
        // filter out topic-level or partition-level metrics
        .filterNot(cm => cm.name.tags.containsKey("topic") || cm.name.tags.containsKey("partition"))
        .toList
        .sortBy(_.name.name())
        .map(_.value.metricValue().toString)

      require(metrics.size == consumerMetricNamesSorted.size,
              "Number of returned metric values DNE number of requested consumer metrics")

      metrics
    }
  }

  private def broker(
      brokersJmx: Seq[MBeanServerConnection],
      brokerMetricNames: List[BrokerMetricRequest]
  )(implicit ec: ExecutionContext): Future[List[String]] = Future {
    brokerMetricNames
      .map { case request @ BrokerMetricRequest(objectName, attrName) =>
        val sum = brokersJmx
          .map(conn => getRemoteJmxValue(objectName, attrName, conn))
          .sum
        BrokerMetricResult(request, sum.toString)
      }
      .sortBy(_.request.name)
      .map(_.value)
  }

  private def getRemoteJmxValue(objectName: String,
                                attrName: String,
                                conn: MBeanServerConnection): Long = {
    val bytesInPerSec: ObjectName = new ObjectName(objectName)
    val attributes = Array(attrName)
    val attributeValues = conn.getAttributes(bytesInPerSec, attributes)
    val attr = attributeValues.stream.findFirst.get.asInstanceOf[Attribute]
    Long.unbox(attr.getValue)
  }
}

private[benchmarks] object InflightMetrics {
  final case class ConsumerMetricResult(name: MetricName, value: Metric)
  final case class BrokerMetricRequest(name: String, attribute: String)
  final case class BrokerMetricResult(request: BrokerMetricRequest, value: String)
}
