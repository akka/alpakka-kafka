package com.softwaremill.react.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization.Deserializer

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.language.postfixOps

object ConsumerProperties {

  /**
   * Consumer Properties
   *
   * bootstrapServers
   * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
   * The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this
   * list only impacts the initial hosts used to discover the full set of servers. This list should be in the
   * form host1:port1,host2:port2,.... Since these servers are just used for the initial connection to discover the full
   * cluster membership (which may change dynamically), this list need not contain the full set of servers
   * (you may want more than one, though, in case a server is down).
   *
   * topic
   * The high-level API hides the details of brokers from the consumer and allows consuming off the cluster of machines
   * without concern for the underlying topology. It also maintains the state of what has been consumed. The high-level API
   * also provides the ability to subscribe to topics that match a filter expression (i.e., either a whitelist or a blacklist
   * regular expression).  This topic is a whitelist only but can change with re-factoring below on the filterSpec
   *
   * groupId
   * A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same
   * group id multiple processes indicate that they are all part of the same consumer group.
   *
   */
  def apply[K, V](
    bootstrapServers: String,
    topic: String,
    groupId: String,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerProperties[K, V] = {
    val props = Map[String, String](
      "group.id" -> groupId,
      KeyBootstrapServers -> bootstrapServers,
      // defaults
      "auto.offset.reset" -> "earliest"
    )

    new ConsumerProperties(props, topic, groupId, keyDeserializer, valueDeserializer)
  }

  val KeyBootstrapServers = "bootstrap.servers"
}

case class ConsumerProperties[K, V](
    params: Map[String, String],
    topic: String,
    groupId: String,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    pollTimeout: FiniteDuration = 500 millis
) {

  /**
   * Use custom interval for auto-commit or commit flushing on manual commit.
   */
  def commitInterval(time: FiniteDuration): ConsumerProperties[K, V] =
    setProperty("auto.commit.interval.ms", time.toMillis.toString)

  /**
   * Consumer Timeout
   * Throw a timeout exception to the consumer if no message is available for consumption after the specified interval
   */
  def consumerTimeoutMs(timeInMs: Long): ConsumerProperties[K, V] = setProperty("consumer.timeout.ms", timeInMs.toString)

  /**
   * What to do when there is no initial offset in Zookeeper or if an offset is out of range:
   * 1) earliest : automatically reset the offset to the smallest offset
   * 2) latest : automatically reset the offset to the largest offset
   * 3) anything else: throw exception to the consumer. If this is set to largest, the consumer may lose some
   * messages when the number of partitions, for the topics it subscribes to, changes on the broker.
   *
   * ***************************************************************************************
   * To prevent data loss during partition addition, set auto.offset.reset to smallest
   *
   * This make sense to change to true if you know you are listening for new data only as of
   * after you connect to the stream new things are coming out.  you can audit/reconcile in
   * another consumer which this flag allows you to toggle if it is catch-up and new stuff or
   * just new stuff coming out of the stream.  This will also block waiting for new stuff so
   * it makes a good listener.
   *
   * //readFromStartOfStream: Boolean = true
   * readFromStartOfStream: Boolean = false
   * ***************************************************************************************
   *
   */
  def readFromEndOfStream(): ConsumerProperties[K, V] = setProperty("auto.offset.reset", "latest")

  def noAutoCommit(): ConsumerProperties[K, V] = setProperty("enable.auto.commit", "false")

  /**
   * Set any additional properties as needed
   */
  def setProperty(key: String, value: String): ConsumerProperties[K, V] = copy(params = params + (key -> value))
  def setProperties(values: (String, String)*): ConsumerProperties[K, V] = copy(params = params ++ values)

  def toProps = params.foldLeft(new Properties()) { (props, param) => props.put(param._1, param._2); props }

  def commitInterval: Option[FiniteDuration] =
    params.get("auto.commit.interval.ms")
      .map(i => new FiniteDuration(i.toLong, TimeUnit.MILLISECONDS))

  /**
   * Dump current props for debugging
   */
  def dump: String = params.map { e => f"${e._1}%-20s : ${e._2.toString}" }.mkString("\n")
}