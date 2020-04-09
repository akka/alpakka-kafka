/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.kafka.ProducerMessage.Envelope
import akka.kafka._
import akka.stream.scaladsl.{Keep, Source}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

object Processor {

  /**
   * Convenience for "at-least-once processing" of a function.
   * Committing is managed by the usual [[Committer]] and the source emits information about batches of committed messages.
   *
   * All records from the given subscription will be processed sequentially.
   */
  def atLeastOnceSource[K, V](
      consumerSettings: ConsumerSettings[K, V],
      subscription: Subscription,
      committerSettings: CommitterSettings
  )(
      process: ConsumerRecord[K, V] => Future[_]
  ): Source[ConsumerMessage.CommittableOffsetBatch, Consumer.Control] = {
    Consumer
      .committableSource(consumerSettings, subscription)
      .mapAsync(1) { message =>
        process(message.record).map(_ => message.committableOffset)(ExecutionContexts.sameThreadExecutionContext)
      }
      .viaMat(Committer.batchFlow(committerSettings))(Keep.left)
  }

  /**
   * Convenience for "at-least-once processing" of a function.
   * Committing is managed by the usual [[Committer]] and the source emits information about batches of committed messages.
   *
   * The `process` function will be called in parallel across Kafka partitions, two records for the same partition will
   * not be processed in parallel.
   *
   * This is similar to a [[Consumer.committablePartitionedSource]].
   *
   * @param parallelism number of parallel "lanes" to distribute messages from the partitions over, the number
   *                    of partitions of the subscribed topic is a good value
   */
  def atLeastOncePerPartitionSource[K, V](
      consumerSettings: ConsumerSettings[K, V],
      subscription: Subscription,
      parallelism: Int,
      committerSettings: CommitterSettings
  )(
      process: ConsumerRecord[K, V] => Future[_]
  ): Source[ConsumerMessage.CommittableOffsetBatch, Consumer.Control] = {
    Consumer
      .committableSource(consumerSettings, subscription)
      .groupBy(parallelism, _.record.partition() % parallelism)
      .mapAsync(1) { message =>
        process(message.record).map(_ => message.committableOffset)(ExecutionContexts.sameThreadExecutionContext)
      }
      .mergeSubstreamsWithParallelism(parallelism)
      .viaMat(Committer.batchFlow(committerSettings))(Keep.left)
  }

  /**
   * Convenience for "at-least-once processing" of Kafka records that may or may not result in new records
   * being produced to Kafka.
   * Committing is managed by the usual [[Committer]] and the source emits information about batches of committed messages.
   */
  def atLeastOnceConsumeAndProduceSource[CK, CV, PK, PV](
      consumerSettings: ConsumerSettings[CK, CV],
      subscription: Subscription,
      producerSettings: ProducerSettings[PK, PV],
      committerSettings: CommitterSettings
  )(
      process: ConsumerRecord[CK, CV] => Future[Envelope[PK, PV, NotUsed]]
  ): Source[ConsumerMessage.CommittableOffsetBatch, Consumer.Control] =
    Consumer
      .committableSource(consumerSettings, subscription)
      .mapAsync(1) { message =>
        process(message.record).map { envelope =>
          envelope.withPassThrough(message.committableOffset)
        }(ExecutionContexts.sameThreadExecutionContext)
      }
      .viaMat(Producer.flexiFlow(producerSettings))(Keep.left)
      .map(_.passThrough)
      .viaMat(Committer.batchFlow(committerSettings))(Keep.left)
}
