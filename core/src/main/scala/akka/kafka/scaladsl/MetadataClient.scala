/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorRef
import akka.kafka.Metadata.{
  BeginningOffsets,
  EndOffsets,
  GetBeginningOffsets,
  GetEndOffsets,
  GetPartitionsFor,
  ListTopics,
  PartitionsFor,
  Topics
}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.concurrent.{ExecutionContext, Future}

object MetadataClient {

  def getBeginningOffsets(
      consumerActor: ActorRef,
      partitions: Set[TopicPartition],
      timeout: Timeout
  )(implicit ec: ExecutionContext): Future[Map[TopicPartition, Long]] =
    (consumerActor ? GetBeginningOffsets(partitions))(timeout)
      .mapTo[BeginningOffsets]
      .map(_.response.get)

  def getBeginningOffsetForPartition(
      consumerActor: ActorRef,
      partition: TopicPartition,
      timeout: Timeout
  )(implicit ec: ExecutionContext): Future[Long] =
    getBeginningOffsets(consumerActor, Set(partition), timeout)
      .map(beginningOffsets => beginningOffsets(partition))

  def getEndOffsets(
      consumerActor: ActorRef,
      partitions: Set[TopicPartition],
      timeout: Timeout
  )(implicit ec: ExecutionContext): Future[Map[TopicPartition, Long]] =
    (consumerActor ? GetEndOffsets(partitions))(timeout)
      .mapTo[EndOffsets]
      .map(_.response.get)

  def getEndOffsetForPartition(
      consumerActor: ActorRef,
      partition: TopicPartition,
      timeout: Timeout
  )(implicit ec: ExecutionContext): Future[Long] =
    getEndOffsets(consumerActor, Set(partition), timeout)
      .map(endOffsets => endOffsets(partition))

  def listTopics(
      consumerActor: ActorRef,
      timeout: Timeout
  )(implicit ec: ExecutionContext): Future[Map[String, List[PartitionInfo]]] =
    (consumerActor ? ListTopics)(timeout)
      .mapTo[Topics]
      .map(_.response.get)

  def getPartitionsFor(
      consumerActor: ActorRef,
      topic: String,
      timeout: Timeout
  )(implicit ec: ExecutionContext): Future[List[PartitionInfo]] =
    (consumerActor ? GetPartitionsFor(topic))(timeout)
      .mapTo[PartitionsFor]
      .map(_.response.get)
}
