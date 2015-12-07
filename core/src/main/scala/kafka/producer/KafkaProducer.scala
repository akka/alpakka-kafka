package kafka.producer

import com.softwaremill.react.kafka.ProducerProperties
import org.apache.kafka.clients.producer.KafkaProducer

case class ReactiveKafkaProducer[K, V](props: ProducerProperties[K, V]) {

  lazy val producer = {
    new KafkaProducer(props.toProperties, props.keySerializer, props.valueSerializer)
  }
}