/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters.FutureOps
import scala.compat.java8.OptionConverters._
import akka.Done
import akka.kafka.javadsl
import akka.kafka.scaladsl
import akka.kafka.PartitionOffset
import akka.kafka.ClientTopicPartition
import java.util.Optional

/**
 * INTERNAL API
 */
private[kafka] class WrappedConsumerControl(underlying: scaladsl.Consumer.Control) extends javadsl.Consumer.Control {
  override def stop(): CompletionStage[Done] = underlying.stop().toJava

  override def shutdown(): CompletionStage[Done] = underlying.shutdown().toJava

  override def isShutdown: CompletionStage[Done] = underlying.isShutdown.toJava
}

/**
 * INTERNAL API
 */
private[kafka] class WrappedCommittableMessage[K, V](underlying: scaladsl.Consumer.CommittableMessage[K, V])
    extends javadsl.Consumer.CommittableMessage[K, V] {

  override def key: K = underlying.key

  override def value: V = underlying.value

  override val committableOffset: javadsl.Consumer.CommittableOffset =
    new WrappedCommittableOffset(underlying.committableOffset)

  override def partitionOffset: PartitionOffset = committableOffset.partitionOffset
}

/**
 * INTERNAL API
 */
private[kafka] class WrappedCommittableOffset(val underlying: scaladsl.Consumer.CommittableOffset)
    extends javadsl.Consumer.CommittableOffset {

  override def partitionOffset: PartitionOffset = underlying.partitionOffset

  override def commit(): CompletionStage[Done] = underlying.commit().toJava
}

/**
 * INTERNAL API
 */
private[kafka] class WrappedConsumerMessage[K, V](underlying: scaladsl.Consumer.Message[K, V])
    extends javadsl.Consumer.Message[K, V] {

  override def key: K = underlying.key

  override def value: V = underlying.value

  override def partitionOffset: PartitionOffset = underlying.partitionOffset
}

/**
 * INTERNAL API
 */
private[kafka] class WrappedCommittableOffsetBatch(underlying: scaladsl.Consumer.CommittableOffsetBatch)
    extends javadsl.Consumer.CommittableOffsetBatch {

  override def updated(offset: javadsl.Consumer.CommittableOffset): javadsl.Consumer.CommittableOffsetBatch =
    offset match {
      case w: WrappedCommittableOffset => new WrappedCommittableOffsetBatch(underlying.updated(w.underlying))
      case other => throw new IllegalArgumentException("Unknown CommittableOffset implementation")
    }

  override def getOffset(key: ClientTopicPartition): Optional[Long] =
    underlying.getOffset(key).asJava

  override def commit(): CompletionStage[Done] = underlying.commit().toJava
}

/**
 * INTERNAL API
 */
private[kafka] class WrappedProducerResult[K, V, PassThrough](underlying: scaladsl.Producer.Result[K, V, PassThrough])
    extends javadsl.Producer.Result[K, V, PassThrough] {

  override def offset: Long = underlying.offset

  override def message: javadsl.Producer.Message[K, V, PassThrough] =
    new javadsl.Producer.Message(underlying.message.record, underlying.message.passThrough)

}
