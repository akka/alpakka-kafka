/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl
import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.{ConsumerSettings, KafkaConsumerActor}
import akka.kafka.Metadata.{BeginningOffsets, GetBeginningOffsets}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ExecutionContext, Future}

object MetadataClient {

  private val consumerActors = scala.collection.mutable.Map[ConsumerSettings[Any, Any], ActorRef]()

  def getBeginningOffsets[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partitions: Set[TopicPartition],
      timeout: Timeout
  )(implicit system: ActorSystem, ec: ExecutionContext): Future[Map[TopicPartition, Long]] = {
    val consumerActor = getConsumerActor(consumerSettings)
    (consumerActor ? GetBeginningOffsets(partitions))(timeout)
      .mapTo[BeginningOffsets]
      .map(_.response.get)
  }

  def getBeginningOffsetForPartition[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partition: TopicPartition,
      timeout: Timeout
  )(implicit system: ActorSystem, ec: ExecutionContext): Future[Long] =
    getBeginningOffsets(consumerSettings, Set(partition), timeout)
      .map(beginningOffsets => beginningOffsets(partition))

  private def getConsumerActor[K, V](consumerSettings: ConsumerSettings[K, V])(implicit system: ActorSystem) = {
    val consumerActor = consumerActors.get(consumerSettings.asInstanceOf[ConsumerSettings[Any, Any]])
    if (consumerActor.isEmpty) {
      val newConsumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))
      consumerActors.put(consumerSettings.asInstanceOf[ConsumerSettings[Any, Any]], newConsumerActor)
      newConsumerActor
    } else {
      consumerActor.get
    }
  }
}
