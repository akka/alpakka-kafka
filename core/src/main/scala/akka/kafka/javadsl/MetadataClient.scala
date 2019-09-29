/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.{CompletionStage, Executor}

import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.compat.java8.FutureConverters._
import scala.collection.compat._
import scala.collection.JavaConverters._

object MetadataClient {

  def getBeginningOffsets(
      consumerActor: ActorRef,
      partitions: java.util.Set[TopicPartition],
      timeout: Timeout,
      executor: Executor
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    akka.kafka.scaladsl.MetadataClient
      .getBeginningOffsets(consumerActor, partitions.asScala.toSet, timeout)
      .map { beginningOffsets =>
        beginningOffsets.view.mapValues(Long.box).toMap.asJava
      }
      .toJava
  }

  def getBeginningOffsetForPartition(
      consumerActor: ActorRef,
      partition: TopicPartition,
      timeout: Timeout,
      executor: Executor
  ): CompletionStage[java.lang.Long] = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    akka.kafka.scaladsl.MetadataClient
      .getBeginningOffsetForPartition(consumerActor, partition, timeout)
      .map(Long.box)
      .toJava
  }
}
