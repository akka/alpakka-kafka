package akka.kafka.benchmarks

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.JavaConversions._

case class KafkaConsumerTestFixture(topic: String, msgCount: Int, consumer: KafkaConsumer[Array[Byte], String]) {
  def close(): Unit = consumer.close()
}

object KafkaConsumerFixtures extends PerfFixtureHelpers {

  def filledTopics(kafkaHost: String, axisName: String)(from: Int, upto: Int, hop: Int) = FixtureGen[KafkaConsumerTestFixture](
    from, upto, hop, msgCount => {
      val topic = randomId()
      fillTopic(kafkaHost, topic, msgCount)
      val consumerJavaProps = new java.util.Properties
      consumerJavaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
      consumerJavaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, randomId())
      consumerJavaProps.put(ConsumerConfig.GROUP_ID_CONFIG, randomId())
      consumerJavaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val consumer = new KafkaConsumer[Array[Byte], String](consumerJavaProps, new ByteArrayDeserializer, new StringDeserializer)
      consumer.subscribe(Set(topic))
      KafkaConsumerTestFixture(topic, msgCount, consumer)
  })
}
