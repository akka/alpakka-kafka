/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.{CompletionStage, Executor}

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerSettings
import akka.util.Timeout
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters._

class MetadataClient private (metadataClient: akka.kafka.scaladsl.MetadataClient) {

  def getBeginningOffsets[K, V](
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] =
    metadataClient
      .getBeginningOffsets(partitions.asScala.toSet)
      .map { beginningOffsets =>
        beginningOffsets.iterator.map { case (k, v) => k -> Long.box(v) }.toMap.asJava
      }(ExecutionContexts.parasitic)
      .toJava

  def getBeginningOffsetForPartition[K, V](partition: TopicPartition): CompletionStage[java.lang.Long] =
    metadataClient
      .getBeginningOffsetForPartition(partition)
      .map(Long.box)(ExecutionContexts.parasitic)
      .toJava

  def getEndOffsets(
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] =
    metadataClient
      .getEndOffsets(partitions.asScala.toSet)
      .map { endOffsets =>
        endOffsets.iterator.map { case (k, v) => k -> Long.box(v) }.toMap.asJava
      }(ExecutionContexts.parasitic)
      .toJava

  def getEndOffsetForPartition(partition: TopicPartition): CompletionStage[java.lang.Long] =
    metadataClient
      .getEndOffsetForPartition(partition)
      .map(Long.box)(ExecutionContexts.parasitic)
      .toJava

  def listTopics(): CompletionStage[java.util.Map[java.lang.String, java.util.List[PartitionInfo]]] =
    metadataClient
      .listTopics()
      .map { topics =>
        topics.view.iterator.map { case (k, v) => k -> v.asJava }.toMap.asJava
      }(ExecutionContexts.parasitic)
      .toJava

  def getPartitionsFor(topic: java.lang.String): CompletionStage[java.util.List[PartitionInfo]] =
    metadataClient
      .getPartitionsFor(topic)
      .map { partitionsInfo =>
        partitionsInfo.asJava
      }(ExecutionContexts.parasitic)
      .toJava

  @deprecated("use `getCommittedOffsets`", "2.0.3")
  def getCommittedOffset(partition: TopicPartition): CompletionStage[OffsetAndMetadata] =
    metadataClient
      .getCommittedOffset(partition)
      .toJava

  def getCommittedOffsets(
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, OffsetAndMetadata]] =
    metadataClient
      .getCommittedOffsets(partitions.asScala.toSet)
      .map { committedOffsets =>
        committedOffsets.asJava
      }(ExecutionContexts.parasitic)
      .toJava

  def close(): Unit =
    metadataClient.close()
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
