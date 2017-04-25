/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.{Map => JMap}
import java.util.concurrent.CompletionStage

import akka.actor.ActorRef
import akka.kafka.ConsumerMessage._
import akka.kafka.KafkaConsumerActor.Internal
import akka.kafka.scaladsl.Consumer._
import akka.kafka.{javadsl, scaladsl, _}
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage.{CallbackWrapper, GraphStageLogic, GraphStageWithMaterializedValue}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.compat.java8.FutureConverters.FutureOps
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * INTERNAL API
 */
private[kafka] object ConsumerStage {
  def plainSubSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription) = {
    new KafkaSourceStage[K, V, (TopicPartition, Source[ConsumerRecord[K, V], NotUsed])] {
      override protected def logic(shape: SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed])]) =
        new SubSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription) with PlainMessageBuilder[K, V]
    }
  }

  def committableSubSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription) = {
    new KafkaSourceStage[K, V, (TopicPartition, Source[CommittableMessage[K, V], NotUsed])] {
      override protected def logic(shape: SourceShape[(TopicPartition, Source[CommittableMessage[K, V], NotUsed])]) =
        new SubSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription) with CommittableMessageBuilder[K, V] {
          override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
          lazy val committer: Committer = {
            val ec = materializer.executionContext
            new KafkaAsyncConsumerCommitterRef(consumer, settings.commitTimeout)(ec)
          }
        }
    }
  }

  def plainSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription) = {
    new KafkaSourceStage[K, V, ConsumerRecord[K, V]] {
      override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]) =
        new SingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription) with PlainMessageBuilder[K, V]
    }
  }

  def externalPlainSource[K, V](consumer: ActorRef, subscription: ManualSubscription) = {
    new KafkaSourceStage[K, V, ConsumerRecord[K, V]] {
      override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]) =
        new ExternalSingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, consumer, subscription) with PlainMessageBuilder[K, V]
    }
  }

  def committableSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription) = {
    new KafkaSourceStage[K, V, CommittableMessage[K, V]] {
      override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
        new SingleSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription) with CommittableMessageBuilder[K, V] {
          override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
          lazy val committer: Committer = {
            val ec = materializer.executionContext
            new KafkaAsyncConsumerCommitterRef(consumer, settings.commitTimeout)(ec)
          }
        }
    }
  }

  def externalCommittableSource[K, V](consumer: ActorRef, _groupId: String, commitTimeout: FiniteDuration, subscription: ManualSubscription) = {
    new KafkaSourceStage[K, V, CommittableMessage[K, V]] {
      override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
        new ExternalSingleSourceLogic[K, V, CommittableMessage[K, V]](shape, consumer, subscription) with CommittableMessageBuilder[K, V] {
          override def groupId: String = _groupId
          lazy val committer: Committer = {
            val ec = materializer.executionContext
            new KafkaAsyncConsumerCommitterRef(consumer, commitTimeout)(ec)
          }
        }
    }
  }

  // This should be case class to be comparable based on ref and timeout. This comparison is used in CommittableOffsetBatchImpl
  case class KafkaAsyncConsumerCommitterRef(ref: ActorRef, timeout: FiniteDuration)(implicit ec: ExecutionContext) extends Committer {
    import akka.pattern.ask
    implicit val to = Timeout(timeout)
    override def commit(offset: PartitionOffset): Future[Done] = {
      val offsets = Map(
        new TopicPartition(offset.key.topic, offset.key.partition) -> (offset.offset + 1)
      )
      (ref ? Internal.Commit(offsets)).mapTo[Internal.CompletedCommit.type].map(_ => Done)
    }
    override def commit(batch: CommittableOffsetBatch): Future[Done] = {
      val offsets = batch.offsets.map {
        case (ctp, offset) => new TopicPartition(ctp.topic, ctp.partition) -> (offset + 1)
      }
      (ref ? Internal.Commit(offsets)).mapTo[Internal.CompletedCommit.type].map(_ => Done)
    }
  }

  abstract class KafkaSourceStage[K, V, Msg]()
      extends GraphStageWithMaterializedValue[SourceShape[Msg], Control] {
    protected val out = Outlet[Msg]("out")
    val shape = new SourceShape(out)
    protected def logic(shape: SourceShape[Msg]): GraphStageLogic with Control
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
      val result = logic(shape)
      (result, result)
    }
  }

  private trait PlainMessageBuilder[K, V] extends MessageBuilder[K, V, ConsumerRecord[K, V]] {
    override def createMessage(rec: ConsumerRecord[K, V]) = rec
  }

  private trait CommittableMessageBuilder[K, V] extends MessageBuilder[K, V, CommittableMessage[K, V]] {
    def groupId: String
    def committer: Committer

    override def createMessage(rec: ConsumerRecord[K, V]) = {
      val offset = ConsumerMessage.PartitionOffset(
        GroupTopicPartition(
          groupId = groupId,
          topic = rec.topic,
          partition = rec.partition
        ),
        offset = rec.offset
      )
      ConsumerMessage.CommittableMessage(rec, CommittableOffsetImpl(offset)(committer))
    }
  }

  final case class CommittableOffsetImpl(override val partitionOffset: ConsumerMessage.PartitionOffset)(val committer: Committer)
      extends CommittableOffset {
    override def commitScaladsl(): Future[Done] = committer.commit(partitionOffset)
    override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava
  }

  trait Committer {
    def commit(offset: PartitionOffset): Future[Done]
    def commit(batch: CommittableOffsetBatch): Future[Done]
  }

  final class CommittableOffsetBatchImpl(val offsets: Map[GroupTopicPartition, Long], val stages: Map[String, Committer])
      extends CommittableOffsetBatch {

    private def updatedStages(
      existing: Map[String, Committer],
      key: GroupTopicPartition,
      committableOffset: CommittableOffset
    ): Map[String, Committer] = {
      val stage = committableOffset match {
        case c: CommittableOffsetImpl => c.committer
        case _ => throw new IllegalArgumentException(
          s"Unknow CommittableOffset, got [${committableOffset.getClass.getName}], " +
            s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
      }

      existing.get(key.groupId) match {
        case Some(s) =>
          require(s == stage, s"CommittableOffset [$committableOffset] origin stage must be same as other " +
            s"stage with same groupId. Expected [$s], got [$stage]")
          existing
        case None =>
          existing.updated(key.groupId, stage)
      }
    }

    private def updatedOffsets(
      existing: Map[GroupTopicPartition, Long],
      key: GroupTopicPartition,
      offset: Long
    ): Map[GroupTopicPartition, Long] = {
      val offsetToUpdateTo = existing.get(key)
        .map(Math.max(_, offset))
        .getOrElse(offset)
      existing.updated(key, offsetToUpdateTo)
    }

    /**
      * Update this batch with a single committable offset. A new CommittableOffsetBatch will be returned that has
      * the in memory map of offsets and commiters updated based on this offset. The offsets map will now contain the
      * largest offset between this committable offset and the existing offsets specified for commit. The commiters
      * map will include the committer for this committable offset
      * @param committableOffset CommittableOffset to be used to update offsets and committers when creating the new
      *                          CommittableOffsetBatch
      * @return
      */
    override def updated(committableOffset: CommittableOffset): CommittableOffsetBatch = {
      val partitionOffset = committableOffset.partitionOffset
      val key = partitionOffset.key

      new CommittableOffsetBatchImpl(updatedOffsets(offsets, key, partitionOffset.offset), updatedStages(stages, key, committableOffset))
    }

    /**
      * Update this batch with multiple committable offsets. A new CommittableOffsetBatch will be returned that has
      * the in memory map of offsets and commiters updated based on these offsets. The offsets map will now contain the
      * largest offset between these committable offsets and the existing offsets specified for commit. The commiters
      * map will include the committers for these committable offsets
      * @param committableOffsets Sequence of CommitableOffset to be used to update offsets and committers when creating
      *                           the new CommittableOffsetBatch
      * @return
      */
    override def updated(committableOffsets: Seq[CommittableOffset]): CommittableOffsetBatch = {
      val (newStages, newOffsets) = committableOffsets.groupBy(_.partitionOffset.key).foldLeft((stages, offsets)) {
        case ((aggStages, aggOffsets), (key, committableOffsetsForKey)) =>
          val commitToUse = committableOffsetsForKey.maxBy(_.partitionOffset.offset)
          val partitionOffset = commitToUse.partitionOffset
          val key = partitionOffset.key

          updatedStages(aggStages, key, commitToUse) -> updatedOffsets(aggOffsets, key, partitionOffset.offset)
      }

      new CommittableOffsetBatchImpl(newOffsets, newStages)
    }

    override def getOffsets(): JMap[GroupTopicPartition, Long] = {
      offsets.asJava
    }

    override def toString(): String =
      s"CommittableOffsetBatch(${offsets.mkString("->")})"

    override def commitScaladsl(): Future[Done] = {
      if (offsets.isEmpty)
        Future.successful(Done)
      else {
        stages.head._2.commit(this)
      }
    }

    override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava

  }

  final class WrappedConsumerControl(underlying: scaladsl.Consumer.Control) extends javadsl.Consumer.Control {
    override def stop(): CompletionStage[Done] = underlying.stop().toJava

    override def shutdown(): CompletionStage[Done] = underlying.shutdown().toJava

    override def isShutdown: CompletionStage[Done] = underlying.isShutdown.toJava
  }
}

private[kafka] trait MessageBuilder[K, V, Msg] {
  def createMessage(rec: ConsumerRecord[K, V]): Msg
}

private[kafka] sealed trait ControlOperation

private[kafka] case object ControlStop extends ControlOperation

private[kafka] case object ControlShutdown extends ControlOperation

private[kafka] trait PromiseControl extends GraphStageLogic with Control with CallbackWrapper[ControlOperation] {

  def shape: SourceShape[_]
  def performShutdown(): Unit
  def performStop(): Unit = {
    setKeepGoing(true)
    complete(shape.out)
    onStop()
  }

  private val shutdownPromise: Promise[Done] = Promise()
  private val stopPromise: Promise[Done] = Promise()

  def onStop() = {
    stopPromise.trySuccess(Done)
  }

  def onShutdown() = {
    stopPromise.trySuccess(Done)
    shutdownPromise.trySuccess(Done)
  }

  override def preStart(): Unit = {
    super.preStart()
    val controlCallback = getAsyncCallback[ControlOperation]({
      case ControlStop => performStop()
      case ControlShutdown => performShutdown()
    })
    initCallback(controlCallback.invoke)
  }

  override def stop(): Future[Done] = {
    invoke(ControlStop)
    stopPromise.future
  }
  override def shutdown(): Future[Done] = {
    invoke(ControlShutdown)
    shutdownPromise.future
  }
  override def isShutdown: Future[Done] = shutdownPromise.future
}
