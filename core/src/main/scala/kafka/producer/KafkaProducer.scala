/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package kafka.producer

import com.softwaremill.react.kafka.ProducerProperties
import org.apache.kafka.clients.producer.KafkaProducer

case class ReactiveKafkaProducer[K, V](props: ProducerProperties[K, V]) {

  val producer = {
    new KafkaProducer(props.rawProperties, props.keySerializer, props.valueSerializer)
  }
}
