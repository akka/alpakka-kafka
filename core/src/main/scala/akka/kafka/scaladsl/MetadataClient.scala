/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorRef
import akka.kafka.Metadata.{BeginningOffsets, GetBeginningOffsets}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

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
}
