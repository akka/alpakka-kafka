package kafka.producer

import com.softwaremill.react.kafka.ProducerProperties
import kafka.producer.async.DefaultEventHandler
import kafka.serializer.Encoder

/**
 * Copied from https://github.com/stealthly/scala-kafka, 0.8.2-beta (not released at the moment)
 */
case class KafkaProducer[T](props: ProducerProperties[T]) {

  val config = props.toProducerConfig
  val producer = new Producer[List[Byte], T](
    props.toProducerConfig,
    new DefaultEventHandler[List[Byte], T](
      config,
      new DefaultPartitioner(),
      props.encoder,
      ByteListEncoder,
      new ProducerPool(config)
    )
  )

  def kafkaMesssage(message: T, partition: Array[Byte]): KeyedMessage[List[Byte], T] = {
    if (partition == null) {
      new KeyedMessage(props.topic, message)
    }
    else {
      new KeyedMessage(props.topic, partition.toList, message)
    }
  }

  def send(message: T, partition: String = null): Unit = send(message, Option(partition).map(_.getBytes("UTF8")))

  def send(message: T, partition: Option[Array[Byte]]): Unit = {
    producer.send(kafkaMesssage(message, partition.orNull))
  }

  def close(): Unit = {
    producer.close()
  }
}

object ByteListEncoder extends Encoder[List[Byte]] {
  override def toBytes(t: List[Byte]) = t.toArray
}