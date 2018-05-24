/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.CompletionStage
import java.util.{Locale, Map => JMap}

import akka.actor.ActorRef
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage._
import akka.kafka.internal.KafkaConsumerActor.Internal.{Commit, Committed, ConsumerMetrics, RequestMetrics}
import akka.kafka.scaladsl.Consumer._
import akka.kafka.{javadsl, scaladsl, _}
import akka.pattern.AskTimeoutException
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.compat.java8.FutureConverters.FutureOps
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * INTERNAL API
 */
private[kafka] object ConsumerStage {
  def plainSubSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription, getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]] = None, onRevoke: Set[TopicPartition] => Unit = _ => ()) = {
    new KafkaSourceStage[K, V, (TopicPartition, Source[ConsumerRecord[K, V], NotUsed])] {
      override protected def logic(shape: SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed])]) =
        new SubSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription, getOffsetsOnAssign, onRevoke) with PlainMessageBuilder[K, V] with MetricsControl
    }
  }

  def committableSubSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription) = {
    new KafkaSourceStage[K, V, (TopicPartition, Source[CommittableMessage[K, V], NotUsed])] {
      override protected def logic(shape: SourceShape[(TopicPartition, Source[CommittableMessage[K, V], NotUsed])]) =
        new SubSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription) with CommittableMessageBuilder[K, V] with MetricsControl {

          override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
          lazy val committer: Committer = {
            val ec = materializer.executionContext
            KafkaAsyncConsumerCommitterRef(consumer, settings.commitTimeout)(ec)
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
        new ExternalSingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, consumer, subscription) with PlainMessageBuilder[K, V] with MetricsControl
    }
  }

  def committableSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription) = {
    new KafkaSourceStage[K, V, CommittableMessage[K, V]] {
      override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
        new SingleSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription) with CommittableMessageBuilder[K, V] {
          override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
          lazy val committer: Committer = {
            val ec = materializer.executionContext
            KafkaAsyncConsumerCommitterRef(consumer, settings.commitTimeout)(ec)
          }
        }
    }
  }

  def externalCommittableSource[K, V](consumer: ActorRef, _groupId: String, commitTimeout: FiniteDuration, subscription: ManualSubscription) = {
    new KafkaSourceStage[K, V, CommittableMessage[K, V]] {
      override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
        new ExternalSingleSourceLogic[K, V, CommittableMessage[K, V]](shape, consumer, subscription) with CommittableMessageBuilder[K, V] with MetricsControl {
          override def groupId: String = _groupId
          lazy val committer: Committer = {
            val ec = materializer.executionContext
            KafkaAsyncConsumerCommitterRef(consumer, commitTimeout)(ec)
          }
        }
    }
  }

  def transactionalSource[K, V](consumerSettings: ConsumerSettings[K, V], subscription: Subscription) = {
    require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty, "You must define a Consumer group.id.")
    /**
     * We set the isolation.level config to read_committed to make sure that any consumed messages are from
     * committed transactions. Note that the consuming partitions may be produced by multiple producers, and these
     * producers may either use transactional messaging or not at all. So the fetching partitions may have both
     * transactional and non-transactional messages, and by setting isolation.level config to read_committed consumers
     * will still consume non-transactional messages.
     */
    val txConsumerSettings = consumerSettings.withProperty(
      ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH))

    new KafkaSourceStage[K, V, TransactionalMessage[K, V]] {
      override protected def logic(shape: SourceShape[TransactionalMessage[K, V]]) =
        new SingleSourceLogic[K, V, TransactionalMessage[K, V]](shape, txConsumerSettings, subscription) with TransactionalMessageBuilder[K, V] {
          override def groupId: String = txConsumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)
        }
    }
  }

  // This should be case class to be comparable based on ref and timeout. This comparison is used in CommittableOffsetBatchImpl
  case class KafkaAsyncConsumerCommitterRef(ref: ActorRef, commitTimeout: FiniteDuration)(implicit ec: ExecutionContext) extends Committer {
    import akka.pattern.ask

    import scala.collection.breakOut
    implicit val to = Timeout(commitTimeout)
    override def commit(offsets: immutable.Seq[PartitionOffset]): Future[Done] = {
      val offsetsMap: Map[TopicPartition, Long] = offsets.map { offset =>
        new TopicPartition(offset.key.topic, offset.key.partition) -> (offset.offset + 1)
      }(breakOut)

      (ref ? Commit(offsetsMap)).mapTo[Committed].map(_ => Done).recoverWith {
        case _: AskTimeoutException => Future.failed(new CommitTimeoutException(s"Kafka commit took longer than: $commitTimeout"))
        case other => Future.failed(other)
      }
    }

    override def commit(batch: CommittableOffsetBatch): Future[Done] = batch match {
      case b: CommittableOffsetBatchImpl =>
        val futures = b.offsets.groupBy(_._1.groupId).map {
          case (groupId, offsetsMap) =>
            val committer = b.stages.getOrElse(
              groupId,
              throw new IllegalStateException(s"Unknown committer, got [$groupId]")
            )
            val offsets: immutable.Seq[PartitionOffset] = offsetsMap.map {
              case (ctp, offset) => PartitionOffset(ctp, offset)
            }(breakOut)
            committer.commit(offsets)
        }
        Future.sequence(futures).map(_ => Done)

      case _ => throw new IllegalArgumentException(
        s"Unknown CommittableOffsetBatch, got [${batch.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
      )
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

  private trait TransactionalMessageBuilder[K, V] extends MessageBuilder[K, V, TransactionalMessage[K, V]] {
    def groupId: String

    override def createMessage(rec: ConsumerRecord[K, V]) = {
      val offset = ConsumerMessage.PartitionOffset(
        GroupTopicPartition(
          groupId = groupId,
          topic = rec.topic,
          partition = rec.partition
        ),
        offset = rec.offset
      )
      ConsumerMessage.TransactionalMessage(rec, offset)
    }
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
    override def commitScaladsl(): Future[Done] = committer.commit(immutable.Seq(partitionOffset))
    override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava
  }

  trait Committer {
    // Commit all offsets (of different topics) belonging to the same stage
    def commit(offsets: immutable.Seq[PartitionOffset]): Future[Done]
    def commit(batch: CommittableOffsetBatch): Future[Done]
  }

  final class CommittableOffsetBatchImpl(val offsets: Map[GroupTopicPartition, Long], val stages: Map[String, Committer])
    extends CommittableOffsetBatch {

    override def updated(committableOffset: CommittableOffset): CommittableOffsetBatch = {
      val partitionOffset = committableOffset.partitionOffset
      val key = partitionOffset.key

      val newOffsets = offsets.updated(key, committableOffset.partitionOffset.offset)

      val stage = committableOffset match {
        case c: CommittableOffsetImpl => c.committer
        case _ => throw new IllegalArgumentException(
          s"Unknown CommittableOffset, got [${committableOffset.getClass.getName}], " +
            s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
      }

      val newStages = stages.get(key.groupId) match {
        case Some(s) =>
          require(s == stage, s"CommittableOffset [$committableOffset] origin stage must be same as other " +
            s"stage with same groupId. Expected [$s], got [$stage]")
          stages
        case None =>
          stages.updated(key.groupId, stage)
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

    override def getMetrics: CompletionStage[java.util.Map[MetricName, Metric]] =
      underlying.metrics.map(_.asJava)(ExecutionContexts.sameThreadExecutionContext).toJava
  }
}

private[kafka] trait MessageBuilder[K, V, Msg] {
  def createMessage(rec: ConsumerRecord[K, V]): Msg
}

private[kafka] sealed trait ControlOperation

private[kafka] case object ControlStop extends ControlOperation

private[kafka] case object ControlShutdown extends ControlOperation

private[kafka] trait PromiseControl extends GraphStageLogic with Control {

  def shape: SourceShape[_]
  def performShutdown(): Unit
  def performStop(): Unit = {
    setKeepGoing(true)
    complete(shape.out)
    onStop()
  }

  private val shutdownPromise: Promise[Done] = Promise()
  private val stopPromise: Promise[Done] = Promise()

  private val controlCallback = getAsyncCallback[ControlOperation]({
    case ControlStop => performStop()
    case ControlShutdown => performShutdown()
  })

  def onStop() = {
    stopPromise.trySuccess(Done)
  }

  def onShutdown() = {
    stopPromise.trySuccess(Done)
    shutdownPromise.trySuccess(Done)
  }

  override def stop(): Future[Done] = {
    controlCallback.invoke(ControlStop)
    stopPromise.future
  }
  override def shutdown(): Future[Done] = {
    controlCallback.invoke(ControlShutdown)
    shutdownPromise.future
  }
  override def isShutdown: Future[Done] = shutdownPromise.future

}

private[kafka] trait MetricsControl extends Control {

  protected def consumer: ActorRef

  def metrics: Future[Map[MetricName, Metric]] = {
    import akka.pattern.ask

    import scala.concurrent.duration._
    consumer.?(RequestMetrics)(Timeout(1.minute)).mapTo[ConsumerMetrics]
      .map(_.metrics)(ExecutionContexts.sameThreadExecutionContext)
  }
}
