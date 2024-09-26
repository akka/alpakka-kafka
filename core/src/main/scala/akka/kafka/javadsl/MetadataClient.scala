/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.{CompletionStage, Executor}

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerSettings
import akka.util.Timeout
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

class MetadataClient private (metadataClient: akka.kafka.scaladsl.MetadataClient) {

  def getBeginningOffsets[K, V](
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] =
    metadataClient
      .getBeginningOffsets(partitions.asScala.toSet)
      .map { beginningOffsets =>
        beginningOffsets.iterator.map { case (k, v) => k -> Long.box(v) }.toMap.asJava
      }(ExecutionContext.parasitic)
      .asJava

  def getBeginningOffsetForPartition[K, V](partition: TopicPartition): CompletionStage[java.lang.Long] =
    metadataClient
      .getBeginningOffsetForPartition(partition)
      .map(Long.box)(ExecutionContext.parasitic)
      .asJava

  def getEndOffsets(
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] =
    metadataClient
      .getEndOffsets(partitions.asScala.toSet)
      .map { endOffsets =>
        endOffsets.iterator.map { case (k, v) => k -> Long.box(v) }.toMap.asJava
      }(ExecutionContext.parasitic)
      .asJava

  def getEndOffsetForPartition(partition: TopicPartition): CompletionStage[java.lang.Long] =
    metadataClient
      .getEndOffsetForPartition(partition)
      .map(Long.box)(ExecutionContext.parasitic)
      .asJava

  def listTopics(): CompletionStage[java.util.Map[java.lang.String, java.util.List[PartitionInfo]]] =
    metadataClient
      .listTopics()
      .map { topics =>
        topics.view.iterator.map { case (k, v) => k -> v.asJava }.toMap.asJava
      }(ExecutionContext.parasitic)
      .asJava

  def getPartitionsFor(topic: java.lang.String): CompletionStage[java.util.List[PartitionInfo]] =
    metadataClient
      .getPartitionsFor(topic)
      .map { partitionsInfo =>
        partitionsInfo.asJava
      }(ExecutionContext.parasitic)
      .asJava

  @deprecated("use `getCommittedOffsets`", "2.0.3")
  def getCommittedOffset(partition: TopicPartition): CompletionStage[OffsetAndMetadata] =
    metadataClient
      .getCommittedOffset(partition)
      .asJava

  def getCommittedOffsets(
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, OffsetAndMetadata]] =
    metadataClient
      .getCommittedOffsets(partitions.asScala.toSet)
      .map { committedOffsets =>
        committedOffsets.asJava
      }(ExecutionContext.parasitic)
      .asJava

  def close(): Unit =
    metadataClient.close()
}

object MetadataClient {

  def create(consumerActor: ActorRef, timeout: Timeout, executor: Executor): MetadataClient = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    val metadataClient = akka.kafka.scaladsl.MetadataClient.create(consumerActor, timeout)
    new MetadataClient(metadataClient)
  }

  def create[K, V](consumerSettings: ConsumerSettings[K, V],
                   timeout: Timeout,
                   system: ActorSystem,
                   executor: Executor): MetadataClient = {
    val metadataClient = akka.kafka.scaladsl.MetadataClient
      .create(consumerSettings, timeout)(system, ExecutionContext.fromExecutor(executor))
    new MetadataClient(metadataClient)
  }
}
