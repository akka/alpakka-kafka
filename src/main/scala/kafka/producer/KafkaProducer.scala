package kafka.producer

/**
 * Copied from https://github.com/stealthly/scala-kafka, 0.8.2-beta (not released at the moment)
 */
case class KafkaProducer(props: ProducerProps) {

  val producer = new Producer[AnyRef, AnyRef](props.toProducerConfig)

  def kafkaMesssage(message: Array[Byte], partition: Array[Byte]): KeyedMessage[AnyRef, AnyRef] = {
    if (partition == null) {
      new KeyedMessage(props.topic, message)
    }
    else {
      new KeyedMessage(props.topic, partition, message)
    }
  }

  def send(message: String, partition: String = null): Unit = send(message.getBytes("UTF8"), Option(partition).map(_.getBytes("UTF8")))

  def send(message: Array[Byte], partition: Option[Array[Byte]]): Unit = {
    producer.send(kafkaMesssage(message, partition.getOrElse(null)))
  }

  def close(): Unit = {
    producer.close()
  }
}
