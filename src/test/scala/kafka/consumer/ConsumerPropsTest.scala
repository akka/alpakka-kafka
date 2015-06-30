package kafka.consumer

import org.scalatest._
import java.util.UUID

class ConsumerPropsTest extends WordSpecLike with Matchers {

  def uuid() = UUID.randomUUID().toString
  val brokerList = "localhost:9092"
  val zooKeepHost = "localhost:2181"
  val topic = uuid()
  val groupId = uuid()

  "ConsumerProps" must {

    "handle base case" in {

      val config = ConsumerProps(brokerList, zooKeepHost, topic, groupId)
        .toConsumerConfig

      config.zkConnect should be(zooKeepHost)
      config.groupId should be(groupId)
      config.clientId should be(groupId)
      config.autoOffsetReset should be("smallest")
      config.offsetsStorage should be("zookeeper")
      config.consumerTimeoutMs should be(1500)
      config.dualCommitEnabled should be(false)
    }

    "handle kafka storage" in {

      val config = ConsumerProps(brokerList, zooKeepHost, topic, groupId)
        .readFromEndOfStream
        .consumerTimeoutMs(1234)
        .kafkaOffsetsStorage(true)
        .toConsumerConfig

      config.zkConnect should be(zooKeepHost)
      config.groupId should be(groupId)
      config.clientId should be(groupId)
      config.autoOffsetReset should be("largest")
      config.offsetsStorage should be("kafka")
      config.dualCommitEnabled should be(true)
      config.consumerTimeoutMs should be(1234)
    }

  }

}