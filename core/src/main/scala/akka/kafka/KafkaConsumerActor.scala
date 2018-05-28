/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.actor.{NoSerializationVerificationNeeded, Props}
import akka.kafka.internal.{KafkaConsumerActor => InternalKafkaConsumerActor}

object KafkaConsumerActor {

  /**
   * Message to send for stopping the Kafka consumer actor.
   */
  case object Stop extends NoSerializationVerificationNeeded

  /**
   * Java API:
   * Message to send for stopping the Kafka consumer actor.
   */
  val stop = Stop

  case class StoppingException() extends RuntimeException("Kafka consumer is stopping")

  def props[K, V](settings: ConsumerSettings[K, V]): Props =
    Props(new InternalKafkaConsumerActor(settings)).withDispatcher(settings.dispatcher)

}
