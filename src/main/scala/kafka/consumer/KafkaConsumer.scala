package kafka.consumer

import kafka.serializer.DefaultDecoder
import kafka.utils.Logging

/**
 * Copied from https://github.com/stealthly/scala-kafka, 0.8.2-beta (not released at the moment)
 */
class KafkaConsumer(val props: ConsumerProps) extends Logging {

  val connector = Consumer.create(props.toConsumerConfig)

  val filterSpec = new Whitelist(props.topic)

  info("setup:start topic=%s for zk=%s and groupId=%s".format(props.topic, props.zookeeperConnect.getOrElse(""), props.groupId))
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())(0)
  info("setup:complete topic=%s for zk=%s and groupId=%s".format(props.topic, props.zookeeperConnect.getOrElse(""), props.groupId))

  def read(write: (Array[Byte]) => Unit) = {
    info("reading on stream now")
    for (messageAndTopic <- stream) {
      try {
        info("writing from stream")
        write(messageAndTopic.message())
        info("written to stream")
      }
      catch {
        case e: Throwable =>
          if (true) { //this is objective even how to conditionalize on it
            error("Error processing message, skipping this message: ", e)
          }
          else {
            throw e
          }
      }
    }
  }

  def iterator() = stream.iterator()

  def close(): Unit = {
    connector.shutdown()
  }
}