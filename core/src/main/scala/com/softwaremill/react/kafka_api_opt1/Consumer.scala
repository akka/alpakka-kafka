/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka_api_opt1

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.stream.Graph
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.Shape
import akka.stream.SourceShape
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

object Consumer {

  // FIXME those should probably be real case classes (or value classes)?
  type CommitMsg = Map[TopicPartition, OffsetAndMetadata]
  type CommitConfirmation = Future[CommitMsg]

  case class ConsumerShape[K, V](
      commit: Inlet[CommitMsg],
      messages: Outlet[ConsumerRecord[K, V]],
      confirmation: Outlet[CommitConfirmation]
  ) extends Shape {
    override def inlets: immutable.Seq[Inlet[_]] = immutable.Seq(commit)
    override def outlets: immutable.Seq[Outlet[_]] = immutable.Seq(messages, confirmation)
    override def deepCopy(): Shape = ConsumerShape(
      commit.carbonCopy(),
      messages.carbonCopy(),
      confirmation.carbonCopy()
    )
    override def copyFromPorts(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]): Shape = {
      require(inlets.size == 1)
      require(outlets.size == 2)
      ConsumerShape(
        inlets.head.as[CommitMsg],
        outlets(0).as[ConsumerRecord[K, V]],
        outlets(1).as[CommitConfirmation]
      )
    }
  }

  trait Control {
    def stop(): Unit
  }

  def record2commit: Flow[ConsumerRecord[_, _], Map[TopicPartition, OffsetAndMetadata], NotUsed] =
    Flow[ConsumerRecord[_, _]]
      .map { msg =>
        Map(new TopicPartition(msg.topic(), msg.partition()) -> new OffsetAndMetadata(msg.offset()))
      }

  def batchCommit(maxCount: Int, maxDuration: FiniteDuration): Flow[CommitMsg, CommitMsg, NotUsed] =
    Flow[Map[TopicPartition, OffsetAndMetadata]]
      .groupedWithin(maxCount, maxDuration)
      .map {
        _.flatten.groupBy(_._1).map {
          case (topic, offsets) => (topic, offsets.map(_._2).maxBy(_.offset()))
        }.toMap
      }

  /**
   * The full graph stage.
   */
  def stage[K, V](consumerProvider: () => KafkaConsumer[K, V]): Graph[ConsumerShape[K, V], Control] = ???

  /**
   * Plain source, commits immediately, i.e. useful when offset is saved in target store.
   */
  def source[K, V](consumerProvider: () => KafkaConsumer[K, V]): Source[ConsumerRecord[K, V], Control] = ???

  /**
   * Don't care about commit confirmations.
   */
  def flow[K, V](consumerProvider: () => KafkaConsumer[K, V]): Flow[CommitMsg, ConsumerRecord[K, V], Control] = ???

  /**
   * Automatically commit when processor flow is done.
   */
  def process[K, V](
    consumerProvider: () => KafkaConsumer[K, V],
    processor: Flow[ConsumerRecord[K, V], ConsumerRecord[K, V], NotUsed]
  ): Source[CommitConfirmation, Control] =
    Source.fromGraph(GraphDSL.create(stage(consumerProvider)) { implicit b => kafka =>
      import GraphDSL.Implicits._
      kafka.messages ~> processor ~> Consumer.record2commit ~> kafka.commit
      SourceShape(kafka.confirmation)
    })

}

