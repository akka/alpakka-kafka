/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.{CompletionStage, Executor}

import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.kafka.common.TopicPartition

import scala.concurrent.ExecutionContext
import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters._

object MetadataClient {

  def getBeginningOffsets(
      consumerActor: ActorRef,
      partitions: java.util.Set[TopicPartition],
      timeout: Timeout,
      executor: Executor
  ): CompletionStage[java.util.Map[TopicPartition, java.lang.Long]] = {
    val ec = ExecutionContext.fromExecutor(executor)
    akka.kafka.scaladsl.MetadataClient
      .getBeginningOffsets(consumerActor, partitions.asScala.toSet, timeout)(ec)
      .map { beginningOffsets =>
        val scalaMapWithJavaValues = beginningOffsets.mapValues(long2Long)
        scalaMapWithJavaValues.asJava
      }(ec)
      .toJava
  }

  def getBeginningOffsetForPartition(
      consumerActor: ActorRef,
      partition: TopicPartition,
      timeout: Timeout,
      executor: Executor
  ): CompletionStage[java.lang.Long] =
    getBeginningOffsets(consumerActor, Set(partition).asJava, timeout, executor)
      .thenApply(beginningOffsets => beginningOffsets.get(partition))
}
