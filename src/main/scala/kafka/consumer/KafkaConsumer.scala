package kafka.consumer

import com.softwaremill.react.kafka.ConsumerProperties
import kafka.serializer.DefaultDecoder
import kafka.utils.Logging

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Copied from https://github.com/stealthly/scala-kafka, 0.8.2-beta (not released at the moment)
 */
class KafkaConsumer[T](val props: ConsumerProperties[T]) extends Logging {

  val connector = Consumer.create(props.toConsumerConfig)
  val filterSpec = new Whitelist(props.topic)

  logger.info(s"setup:start topic=${props.topic} for zk=${props.zookeeperConnect} and groupId=${props.groupId}")
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), props.decoder).head
  logger.info(s"setup:complete topic=${props.topic} for zk=${props.zookeeperConnect} and groupId=${props.groupId}")

  def iterator() = stream.iterator()

  def close(): Unit = {
    connector.shutdown()
  }

  def commitInterval = props.commitInterval.getOrElse(KafkaConsumer.DefaultCommitInterval)

  def kafkaOffsetStorage = props.kafkaOffsetStorage
}

object KafkaConsumer {
  val DefaultCommitInterval = 30 seconds
}