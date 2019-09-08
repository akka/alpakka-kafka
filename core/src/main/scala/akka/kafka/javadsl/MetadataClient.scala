/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.{CompletionStage, Executor}

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.concurrent.ExecutionContext
import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters._

object MetadataClient {

  def getBeginningOffsets[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partitions: java.util.Set[TopicPartition],
      timeout: Timeout,
      system: ActorSystem,
      executor: Executor
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] = {
    val ec = ExecutionContext.fromExecutor(executor)
    akka.kafka.scaladsl.MetadataClient
      .getBeginningOffsets(consumerSettings, partitions.asScala.toSet, timeout)(system, ec)
      .map { beginningOffsets =>
        val scalaMapWithJavaValues = beginningOffsets.mapValues(long2Long)
        scalaMapWithJavaValues.asJava
      }(ec)
      .toJava
  }

  def getBeginningOffsetForPartition[K, V](
      consumerSettings: ConsumerSettings[K, V],
      partition: TopicPartition,
      timeout: Timeout,
      system: ActorSystem,
      executor: Executor
  ): CompletionStage[java.lang.Long] =
    getBeginningOffsets(consumerSettings, Set(partition).asJava, timeout, system, executor)
      .thenApply(_.get(partition))
}
