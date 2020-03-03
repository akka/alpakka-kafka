/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.PartitionOffset
import akka.kafka.ProducerMessage.MultiMessage
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.scalatest.{Matchers, TestSuite}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

trait TransactionsOps extends TestSuite with Matchers {
  def transactionalCopyStream(
      consumerSettings: ConsumerSettings[String, String],
      producerSettings: ProducerSettings[String, String],
      sourceTopic: String,
      sinkTopic: String,
      transactionalId: String,
      idleTimeout: FiniteDuration,
      restartAfter: Option[Int] = None
  ): Source[ProducerMessage.Results[String, String, PartitionOffset], Control] =
    Transactional
      .source(consumerSettings, Subscriptions.topics(sourceTopic))
      .zip(Source.unfold(1)(count => Some((count + 1, count))))
      .map {
        case (msg, count) =>
          if (restartAfter.exists(restartAfter => count >= restartAfter))
            throw new Error("Restarting transactional copy stream")
          msg
      }
      .idleTimeout(idleTimeout)
      .map { msg =>
        ProducerMessage.single(new ProducerRecord[String, String](sinkTopic, msg.record.value), msg.partitionOffset)
      }
      .via(Transactional.flow(producerSettings, transactionalId))

  /**
   * Copy messages from a source to sink topic. Source and sink must have exactly the same number of partitions.
   */
  def transactionalPartitionedCopyStream(
      consumerSettings: ConsumerSettings[String, String],
      producerSettings: ProducerSettings[String, String],
      sourceTopic: String,
      sinkTopic: String,
      transactionalId: String,
      idleTimeout: FiniteDuration,
      maxPartitions: Int,
      restartAfter: Option[Int] = None
  ): Source[ProducerMessage.Results[String, String, PartitionOffset], Control] =
    Transactional
      .partitionedSource(consumerSettings, Subscriptions.topics(sourceTopic))
      .flatMapMerge(
        maxPartitions, {
          case (_, source) =>
            val results: Source[ProducerMessage.Results[String, String, PartitionOffset], NotUsed] = source
              .zip(Source.unfold(1)(count => Some((count + 1, count))))
              .map {
                case (msg, count) =>
                  if (restartAfter.exists(restartAfter => count >= restartAfter))
                    throw new Error("Restarting transactional copy stream")
                  msg
              }
              .idleTimeout(idleTimeout)
              .map { msg =>
                ProducerMessage.single(new ProducerRecord[String, String](sinkTopic,
                                                                          msg.record.partition(),
                                                                          msg.record.key(),
                                                                          msg.record.value),
                                       msg.partitionOffset)
              }
              .via(Transactional.flow(producerSettings, transactionalId))
            results
        }
      )

  def produceToAllPartitions(producerSettings: ProducerSettings[String, String],
                             topic: String,
                             partitions: Int,
                             range: Range)(implicit mat: Materializer): Future[Done] =
    Source(range)
      .map { n =>
        val msgs = (0 until partitions).map(p => new ProducerRecord(topic, p, n.toString, n.toString))
        MultiMessage(msgs, n)
      }
      .via(Producer.flexiFlow(producerSettings))
      .runWith(Sink.ignore)

  def checkForDuplicates(values: immutable.Seq[(Long, String)], expected: immutable.IndexedSeq[String]): Unit =
    withClue("Checking for duplicates: ") {
      val duplicates = values.map(_._2) diff expected
      if (duplicates.nonEmpty) {
        val duplicatesWithDifferentOffsets = values
          .filter {
            case (_, value) => duplicates.contains(value)
          }
          .groupBy(_._2) // message
          // workaround for Scala collection refactoring of `mapValues` to remain compat with 2.12/2.13 cross build
          .map { case (k, v) => (k, v.map(_._1)) } // keep offset
          .filter {
            case (_, offsets) => offsets.distinct.size > 1
          }

        if (duplicatesWithDifferentOffsets.nonEmpty) {
          fail(s"Got ${duplicates.size} duplicates. Messages and their offsets: $duplicatesWithDifferentOffsets")
        } else {
          println("Got duplicates, but all of them were due to rebalance replay when counting")
        }
      }
    }

  def checkForMissing(values: immutable.Seq[(Long, String)], expected: immutable.IndexedSeq[String]): Unit =
    withClue("Checking for missing: ") {
      val missing = expected diff values.map(_._2)
      if (missing.nonEmpty) {
        val continuousBlocks = missing
          .scanLeft(("-1", 0)) {
            case ((last, block), curr) => if (last.toInt + 1 == curr.toInt) (curr, block) else (curr, block + 1)
          }
          .tail
          .groupBy(_._2)
        val blockDescription = continuousBlocks
          .map { block =>
            val msgs = block._2.map(_._1)
            s"Missing ${msgs.size} in continuous block, first ten: ${msgs.take(10)}"
          }
          .mkString(" ")
        fail(s"Did not get ${missing.size} expected messages. $blockDescription")
      }
    }

  def valuesProbeConsumer(
      settings: ConsumerSettings[String, String],
      topic: String
  )(implicit actorSystem: ActorSystem, mat: Materializer): TestSubscriber.Probe[String] =
    offsetValueSource(settings, topic)
      .map(_._2)
      .runWith(TestSink.probe)

  def offsetValueSource(settings: ConsumerSettings[String, String],
                        topic: String): Source[(Long, String), Consumer.Control] =
    Consumer
      .plainSource(settings, Subscriptions.topics(topic))
      .map(r => (r.offset(), r.value()))

  def consumePartitionOffsetValues(settings: ConsumerSettings[String, String], topic: String, elementsToTake: Long)(
      implicit mat: Materializer
  ): Future[immutable.Seq[(Int, Long, String)]] =
    Consumer
      .plainSource(settings, Subscriptions.topics(topic))
      .map(r => (r.partition(), r.offset(), r.value()))
      .take(elementsToTake)
      .idleTimeout(30.seconds)
      .alsoTo(
        Flow[(Int, Long, String)]
          .scan(0) { case (count, _) => count + 1 }
          .filter(_ % 100 == 0)
          .log("received")
          .to(Sink.ignore)
      )
      .recover {
        case t => (0, 0L, "no-more-elements")
      }
      .filter(_._3 != "no-more-elements")
      .runWith(Sink.seq)

  def assertPartitionedConsistency(
      elements: Int,
      maxPartitions: Int,
      values: immutable.Seq[(Int, Long, String)]
  ): Unit = {
    val expectedValues: immutable.Seq[String] = (1 to elements).map(_.toString)

    for (partition <- 0 until maxPartitions) {
      println(s"Asserting values for partition: $partition")

      val partitionMessages: immutable.Seq[String] =
        values.filter(_._1 == partition).map { case (_, _, value) => value }

      assert(partitionMessages.length == elements)
      expectedValues should contain theSameElementsInOrderAs partitionMessages
    }
  }

  def withProbeConsumerSettings(settings: ConsumerSettings[String, String],
                                groupId: String): ConsumerSettings[String, String] =
    TransactionsOps.withProbeConsumerSettings(settings, groupId)

  def withTestProducerSettings(settings: ProducerSettings[String, String]): ProducerSettings[String, String] =
    TransactionsOps.withTestProducerSettings(settings)

  def withTransactionalProducerSettings(settings: ProducerSettings[String, String]): ProducerSettings[String, String] =
    TransactionsOps.withTransactionalProducerSettings(settings)
}

object TransactionsOps {
  def withProbeConsumerSettings(settings: ConsumerSettings[String, String],
                                groupId: String): ConsumerSettings[String, String] =
    settings
      .withGroupId(groupId)
      .withProperties(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")

  def withTestProducerSettings(settings: ProducerSettings[String, String]): ProducerSettings[String, String] =
    settings
      .withCloseTimeout(Duration.Zero)
      .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

  def withTransactionalProducerSettings(settings: ProducerSettings[String, String]): ProducerSettings[String, String] =
    settings
      .withParallelism(20)
      .withCloseTimeout(Duration.Zero)
}
