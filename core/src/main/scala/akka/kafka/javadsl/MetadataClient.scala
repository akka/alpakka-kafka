/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.{CompletionStage, Executor}

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerSettings
import akka.util.Timeout
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.compat.java8.FutureConverters._
import scala.collection.compat._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

class MetadataClient private (metadataClient: akka.kafka.scaladsl.MetadataClient) {

  def getBeginningOffsets[K, V](
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] =
    metadataClient
      .getBeginningOffsets(partitions.asScala.toSet)
      .map { beginningOffsets =>
        beginningOffsets.view.mapValues(Long.box).toMap.asJava
      }(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def getBeginningOffsetForPartition[K, V](partition: TopicPartition): CompletionStage[java.lang.Long] =
    metadataClient
      .getBeginningOffsetForPartition(partition)
      .map(Long.box)(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def getEndOffsets(
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] =
    metadataClient
      .getEndOffsets(partitions.asScala.toSet)
      .map { endOffsets =>
        endOffsets.view.mapValues(Long.box).toMap.asJava
      }(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def getEndOffsetForPartition(partition: TopicPartition): CompletionStage[java.lang.Long] =
    metadataClient
      .getEndOffsetForPartition(partition)
      .map(Long.box)(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def listTopics(): CompletionStage[java.util.Map[java.lang.String, java.util.List[PartitionInfo]]] =
    metadataClient
      .listTopics()
      .map { topics =>
        topics.view.mapValues(partitionsInfo => partitionsInfo.asJava).toMap.asJava
      }(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def getPartitionsFor(topic: java.lang.String): CompletionStage[java.util.List[PartitionInfo]] =
    metadataClient
      .getPartitionsFor(topic)
      .map { partitionsInfo =>
        partitionsInfo.asJava
      }(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def stop(): Unit =
    metadataClient.stop()
}

object MetadataClient {

  def create(consumerActor: ActorRef, timeout: Timeout, executor: Executor): MetadataClient = {
    implicit val ec: ExecutionContextExecutor = ExecutionContexts.fromExecutor(executor)
    val metadataClient = akka.kafka.scaladsl.MetadataClient.create(consumerActor, timeout)
    new MetadataClient(metadataClient)
  }

  def create[K, V](consumerSettings: ConsumerSettings[K, V],
                   timeout: Timeout,
                   system: ActorSystem,
                   executor: Executor): MetadataClient = {
    val metadataClient = akka.kafka.scaladsl.MetadataClient
      .create(consumerSettings, timeout)(system, ExecutionContexts.fromExecutor(executor))
    new MetadataClient(metadataClient)
  }
}
