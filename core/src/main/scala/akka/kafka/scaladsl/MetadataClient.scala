/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.ExecutionContexts
import akka.kafka.{ConsumerSettings, KafkaConsumerActor}
import akka.kafka.Metadata.{BeginningOffsets, GetBeginningOffsets}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class MetadataClient(actorSystem: ActorSystem, timeout: Timeout) {

  private val consumerActors = scala.collection.mutable.Map[ConsumerSettings[Any, Any], ActorRef]()
  private implicit val system: ActorSystem = actorSystem
  private implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  def getBeginningOffsets[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partitions: Set[TopicPartition]
  ): Future[Map[TopicPartition, Long]] = {
    val consumerActor: ActorRef = getConsumerActor(consumerSettings)
    (consumerActor ? GetBeginningOffsets(partitions))(timeout)
      .mapTo[BeginningOffsets]
      .map(_.response)
      .flatMap {
        case Success(res) => Future.successful(res)
        case Failure(e) => Future.failed(e)
      }(ExecutionContexts.sameThreadExecutionContext)
  }

  def getBeginningOffsetForPartition[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partition: TopicPartition
  ): Future[Long] =
    getBeginningOffsets(consumerSettings, Set(partition))
      .map(beginningOffsets => beginningOffsets(partition))

  def stopConsumerActor[K, V](consumerSettings: ConsumerSettings[K, V]): Unit = {
    val consumerActor = getConsumerActor(consumerSettings)
    consumerActor ! KafkaConsumerActor.Stop
  }

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
