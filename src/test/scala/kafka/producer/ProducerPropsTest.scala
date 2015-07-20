package kafka.producer

import com.softwaremill.react.kafka.ProducerProps
import kafka.serializer.StringEncoder
import org.scalatest._
import java.util.UUID

class ProducerPropsTest extends WordSpecLike with Matchers {

  def uuid() = UUID.randomUUID().toString
  val brokerList = "localhost:9092"
  val topic = uuid()
  val clientId = uuid()

  "ProducerProps" must {

    "handle base case" in {

      val config = ProducerProps(brokerList, topic, clientId, new StringEncoder())
        .toProducerConfig

      config.brokerList should be(brokerList)
      config.compressionCodec.name should be("gzip")
      config.producerType should be("sync")
      config.clientId should be(clientId)
      config.messageSendMaxRetries should be(3)
      config.requestRequiredAcks should be(-1)
      config.batchNumMessages should be(200) // kafka defaults
      config.queueBufferingMaxMs should be(5000) // kafka defaults
    }

    "handle async snappy case" in {

      val config = ProducerProps(brokerList, topic, clientId, new StringEncoder())
        .asynchronous(123, 456)
        .useSnappyCompression()
        .toProducerConfig

      config.brokerList should be(brokerList)
      config.compressionCodec.name should be("snappy")
      config.producerType should be("async")
      config.clientId should be(clientId)
      config.messageSendMaxRetries should be(3)
      config.requestRequiredAcks should be(-1)
      config.batchNumMessages should be(123)
      config.queueBufferingMaxMs should be(456)
    }
  }

}