/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.ExecutionContexts
import akka.kafka.Metadata.{BeginningOffsets, EndOffsets, GetBeginningOffsets, GetEndOffsets, ListTopics, Topics}
import akka.kafka.{ConsumerSettings, KafkaConsumerActor}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class MetadataClient private (consumerActor: ActorRef, timeout: Timeout)(implicit ec: ExecutionContext) {

  def getBeginningOffsets(partitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
    (consumerActor ? GetBeginningOffsets(partitions))(timeout)
      .mapTo[BeginningOffsets]
      .map(_.response)
      .flatMap {
        case Success(res) => Future.successful(res)
        case Failure(e) => Future.failed(e)
      }(ExecutionContexts.sameThreadExecutionContext)

  def getBeginningOffsetForPartition(partition: TopicPartition): Future[Long] =
    getBeginningOffsets(Set(partition))
      .map(beginningOffsets => beginningOffsets(partition))

  def getEndOffsets(partitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
    (consumerActor ? GetEndOffsets(partitions))(timeout)
      .mapTo[EndOffsets]
      .map(_.response)
      .flatMap {
        case Success(res) => Future.successful(res)
        case Failure(e) => Future.failed(e)
      }(ExecutionContexts.sameThreadExecutionContext)

  def getEndOffsetForPartition(partition: TopicPartition): Future[Long] =
    getEndOffsets(Set(partition))
      .map(endOffsets => endOffsets(partition))

  def listTopics(): Future[Map[String, List[PartitionInfo]]] =
    (consumerActor ? ListTopics)(timeout)
      .mapTo[Topics]
      .map(_.response.get)

  def stop(): Unit =
    consumerActor ! KafkaConsumerActor.Stop
}

object MetadataClient {

  def create(consumerActor: ActorRef, timeout: Timeout)(implicit ec: ExecutionContext): MetadataClient =
    new MetadataClient(consumerActor, timeout)

  def create[K, V](
      consumerSettings: ConsumerSettings[K, V],
      timeout: Timeout
  )(implicit system: ActorSystem, ec: ExecutionContext): MetadataClient = {
    val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerSettings))
    new MetadataClient(consumerActor, timeout)
  }
}
