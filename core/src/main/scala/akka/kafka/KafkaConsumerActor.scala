/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import akka.actor.{ActorRef, NoSerializationVerificationNeeded, Props}
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

  /**
   * Creates Props for the Kafka Consumer Actor.
   */
  def props[K, V](settings: ConsumerSettings[K, V]): Props =
    Props(new InternalKafkaConsumerActor(None, settings)).withDispatcher(settings.dispatcher)

  /**
   * Creates Props for the Kafka Consumer Actor with a reference back to the owner of it
   * which will be signalled with [[akka.actor.Status.Failure(exception)]], in case the
   * Kafka client instance can't be created.
   */
  def props[K, V](owner: ActorRef, settings: ConsumerSettings[K, V]): Props =
    Props(new InternalKafkaConsumerActor(Some(owner), settings)).withDispatcher(settings.dispatcher)
}
