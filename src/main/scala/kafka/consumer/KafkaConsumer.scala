package kafka.consumer

import com.softwaremill.react.kafka.ConsumerProperties
import kafka.serializer.DefaultDecoder
import kafka.utils.Logging

/**
 * Copied from https://github.com/stealthly/scala-kafka, 0.8.2-beta (not released at the moment)
 */
class KafkaConsumer[K, V](val props: ConsumerProperties[K, V]) extends Logging {

  val connector = Consumer.create(props.toConsumerConfig)
  val filterSpec = new Whitelist(props.topic)

  info("setup:start topic=%s for zk=%s and groupId=%s".format(props.topic, props.zookeeperConnect.getOrElse(""), props.groupId))
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, props.keyDecoder, props.decoder).head
  info("setup:complete topic=%s for zk=%s and groupId=%s".format(props.topic, props.zookeeperConnect.getOrElse(""), props.groupId))

  def iterator() = stream.iterator()

  def close(): Unit = {
    connector.shutdown()
  }
}