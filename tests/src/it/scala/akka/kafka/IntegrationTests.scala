package akka.kafka

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.testcontainers.containers.GenericContainer

import scala.collection.JavaConverters._

object IntegrationTests {
  val MessageLogInterval = 500L

  def logSentMessages()
                     (implicit log: Logger): Flow[Long, Long, NotUsed] = Flow[Long].map { i =>
    if (i % MessageLogInterval == 0) log.info(s"Sent [$i] messages so far.")
    i
  }

  def logReceivedMessages()
                         (implicit log: Logger): Flow[Long, Long, NotUsed] = Flow[Long].map { i =>
    if (i % MessageLogInterval == 0) log.info(s"Received [$i] messages so far.")
    i
  }

  def logReceivedMessages(tp: TopicPartition)
                         (implicit log: Logger): Flow[Long, Long, NotUsed] = Flow[Long].map { i =>
    if (i % MessageLogInterval == 0) log.info(s"$tp: Received [$i] messages so far.")
    i
  }

  def stopRandomBroker(brokers: Vector[GenericContainer[_]], msgCount: Long)
                      (implicit log: Logger): Unit = {
    val broker: GenericContainer[_] = brokers(scala.util.Random.nextInt(brokers.length))
    val id = broker.getContainerId
    val networkAliases = broker.getNetworkAliases.asScala.mkString(",")
    log.warn(s"Stopping one Kafka container with network aliases [$networkAliases], container id [$id], after [$msgCount] messages")
    broker.stop()
  }

}
