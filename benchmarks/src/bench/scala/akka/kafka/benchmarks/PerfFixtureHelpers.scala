package akka.kafka.benchmarks

import java.util.UUID

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringSerializer, ByteArraySerializer}

private[benchmarks] trait PerfFixtureHelpers {

  def randomId() = UUID.randomUUID().toString

  def fillTopic(kafkaHost: String, topic: String, msgCount: Int): Unit = {
    val producerJavaProps = new java.util.Properties
    producerJavaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
    val producer = new KafkaProducer[Array[Byte], String](producerJavaProps, new ByteArraySerializer, new StringSerializer)
    for (i <- 0 to msgCount)
      producer.send(new ProducerRecord[Array[Byte], String](topic, i.toString))
    producer.close()
  }
}
