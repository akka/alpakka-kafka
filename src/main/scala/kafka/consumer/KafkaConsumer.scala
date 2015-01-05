package kafka.consumer

import java.util.Properties

import kafka.serializer.DefaultDecoder
import kafka.utils.Logging

/**
 * Copied from https://github.com/stealthly/scala-kafka, 0.8.2-beta (not released at the moment)
 */
class KafkaConsumer(
                     topic: String,
                     /** topic
                       * The high-level API hides the details of brokers from the consumer and allows consuming off the cluster of machines
                       * without concern for the underlying topology. It also maintains the state of what has been consumed. The high-level API
                       * also provides the ability to subscribe to topics that match a filter expression (i.e., either a whitelist or a blacklist
                       * regular expression).  This topic is a whitelist only but can change with re-factoring below on the filterSpec
                       */
                     groupId: String,
                     /** groupId
                       * A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same
                       * group id multiple processes indicate that they are all part of the same consumer group.
                       */
                     zookeeperConnect: String,
                     /**
                      * Specifies the zookeeper connection string in the form hostname:port where host and port are the host and port of
                      * a zookeeper server. To allow connecting through other zookeeper nodes when that zookeeper machine is down you can also
                      * specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3. The server may also have a zookeeper
                      * chroot path as part of it's zookeeper connection string which puts its data under some path in the global zookeeper namespace.
                      * If so the consumer should use the same chroot path in its connection string. For example to give a chroot path of /chroot/path
                      * you would give the connection string as hostname1:port1,hostname2:port2,hostname3:port3/chroot/path.
                      */
                     readFromStartOfStream: Boolean = true
                     /**
                      * What to do when there is no initial offset in Zookeeper or if an offset is out of range:
                      * 1) smallest : automatically reset the offset to the smallest offset
                      * 2) largest : automatically reset the offset to the largest offset
                      * 3) anything else: throw exception to the consumer. If this is set to largest, the consumer may lose some
       messages when the number of partitions, for the topics it subscribes to, changes on the broker.

                      ****************************************************************************************
  To prevent data loss during partition addition, set auto.offset.reset to smallest

  This make sense to change to true if you know you are listening for new data only as of
  after you connect to the stream new things are coming out.  you can audit/reconcile in
  another consumer which this flag allows you to toggle if it is catch-up and new stuff or
  just new stuff coming out of the stream.  This will also block waiting for new stuff so
  it makes a good listener.

  //readFromStartOfStream: Boolean = true
  readFromStartOfStream: Boolean = false
                      ****************************************************************************************

                      */
                     ) extends Logging {

  val props = new Properties()
  props.put("group.id", groupId)
  props.put("zookeeper.connect", zookeeperConnect)
  props.put("auto.offset.reset", if(readFromStartOfStream) "smallest" else "largest")
  props.put("consumer.timeout.ms", "500")
  val config = new ConsumerConfig(props)
  val connector = Consumer.create(config)

  val filterSpec = new Whitelist(topic)

  info("setup:start topic=%s for zk=%s and groupId=%s".format(topic,zookeeperConnect,groupId))
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())(0)
  info("setup:complete topic=%s for zk=%s and groupId=%s".format(topic,zookeeperConnect,groupId))

  def read(write: (Array[Byte])=>Unit) = {
    info("reading on stream now")
    for(messageAndTopic <- stream) {
      try {
        info("writing from stream")
        write(messageAndTopic.message())
        info("written to stream")
      } catch {
        case e: Throwable =>
          if (true) { //this is objective even how to conditionalize on it
            error("Error processing message, skipping this message: ", e)
          } else {
            throw e
          }
      }
    }
  }

  def iterator() = stream.iterator()

  def close() {
    connector.shutdown()
  }
}