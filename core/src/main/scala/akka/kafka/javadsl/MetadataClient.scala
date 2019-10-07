/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerSettings
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.compat.java8.FutureConverters._
import scala.collection.compat._
import scala.collection.JavaConverters._

class MetadataClient(actorSystem: ActorSystem, timeout: Timeout) {

  private val metadataClient = new akka.kafka.scaladsl.MetadataClient(actorSystem, timeout)

  def getBeginningOffsets[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partitions: java.util.Set[TopicPartition]
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] =
    metadataClient
      .getBeginningOffsets(consumerSettings, partitions.asScala.toSet)
      .map { beginningOffsets =>
        beginningOffsets.view.mapValues(Long.box).toMap.asJava
      }(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def getBeginningOffsetForPartition[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partition: TopicPartition
  ): CompletionStage[java.lang.Long] =
    metadataClient
      .getBeginningOffsetForPartition(consumerSettings, partition)
      .map(Long.box)(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  def stopConsumerActor[K, V](consumerSettings: ConsumerSettings[K, V]): Unit =
    metadataClient.stopConsumerActor(consumerSettings)

}
