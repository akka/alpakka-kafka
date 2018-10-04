/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.{CompletionStage, Executor}
import java.util.{Locale, Map => JMap}

import akka.actor.ActorRef
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage._
import akka.kafka.internal.KafkaConsumerActor.Internal.{Commit, ConsumerMetrics, RequestMetrics}
import akka.kafka.scaladsl.Consumer._
import akka.kafka.{javadsl, scaladsl, _}
import akka.pattern.AskTimeoutException
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.requests.{IsolationLevel, OffsetFetchResponse}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.compat.java8.FutureConverters.{CompletionStageOps, FutureOps}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * INTERNAL API
 */
private[kafka] object ConsumerStage {
  private[kafka] final class PlainSubSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription,
      getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]],
      onRevoke: Set[TopicPartition] => Unit
  ) extends KafkaSourceStage[K, V, (TopicPartition, Source[ConsumerRecord[K, V], NotUsed])](
        s"PlainSubSource ${subscription.renderStageAttribute}"
      ) {
    override protected def logic(shape: SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed])]) =
      new SubSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription, getOffsetsOnAssign, onRevoke)
      with PlainMessageBuilder[K, V] with MetricsControl
  }

  private[kafka] final class CommittableSubSource[K, V](settings: ConsumerSettings[K, V],
                                                        subscription: AutoSubscription,
                                                        _metadataFromRecord: ConsumerRecord[K, V] => String =
                                                          (_: ConsumerRecord[K, V]) => OffsetFetchResponse.NO_METADATA)
      extends KafkaSourceStage[K, V, (TopicPartition, Source[CommittableMessage[K, V], NotUsed])](
        s"CommittableSubSource ${subscription.renderStageAttribute}"
      ) {
    override protected def logic(shape: SourceShape[(TopicPartition, Source[CommittableMessage[K, V], NotUsed])]) =
      new SubSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription)
      with CommittableMessageBuilder[K, V] with MetricsControl {
        override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
        override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
        lazy val committer: Committer = {
          val ec = materializer.executionContext
          KafkaAsyncConsumerCommitterRef(consumerActor, settings.commitTimeout)(ec)
        }
      }
  }

  private[kafka] final class PlainSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription)
      extends KafkaSourceStage[K, V, ConsumerRecord[K, V]](s"PlainSource ${subscription.renderStageAttribute}") {
    override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]) =
      new SingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription) with PlainMessageBuilder[K, V]
  }

  private[kafka] final class ExternalPlainSource[K, V](consumer: ActorRef, subscription: ManualSubscription)
      extends KafkaSourceStage[K, V, ConsumerRecord[K, V]](
        s"ExternalPlainSubSource ${subscription.renderStageAttribute}"
      ) {
    override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]) =
      new ExternalSingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, consumer, subscription)
      with PlainMessageBuilder[K, V] with MetricsControl
  }

  private[kafka] final class CommittableSource[K, V](settings: ConsumerSettings[K, V],
                                                     subscription: Subscription,
                                                     _metadataFromRecord: ConsumerRecord[K, V] => String =
                                                       (_: ConsumerRecord[K, V]) => OffsetFetchResponse.NO_METADATA)
      extends KafkaSourceStage[K, V, CommittableMessage[K, V]](
        s"CommittableSource ${subscription.renderStageAttribute}"
      ) {
    override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
      new SingleSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription)
      with CommittableMessageBuilder[K, V] {
        override def metadataFromRecord(record: ConsumerRecord[K, V]): String = _metadataFromRecord(record)
        override def groupId: String = settings.properties(ConsumerConfig.GROUP_ID_CONFIG)
        lazy val committer: Committer = {
          val ec = materializer.executionContext
          KafkaAsyncConsumerCommitterRef(consumerActor, settings.commitTimeout)(ec)
        }
      }
  }

  private[kafka] final class ExternalCommittableSource[K, V](consumer: ActorRef,
                                                             _groupId: String,
                                                             commitTimeout: FiniteDuration,
                                                             subscription: ManualSubscription)
      extends KafkaSourceStage[K, V, CommittableMessage[K, V]](
        s"ExternalCommittableSource ${subscription.renderStageAttribute}"
      ) {
    override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
      new ExternalSingleSourceLogic[K, V, CommittableMessage[K, V]](shape, consumer, subscription)
      with CommittableMessageBuilder[K, V] {
        override def metadataFromRecord(record: ConsumerRecord[K, V]): String = OffsetFetchResponse.NO_METADATA
        override def groupId: String = _groupId
        lazy val committer: Committer = {
          val ec = materializer.executionContext
          KafkaAsyncConsumerCommitterRef(consumerActor, commitTimeout)(ec)
        }
      }
  }

  private[kafka] final class TransactionalSource[K, V](consumerSettings: ConsumerSettings[K, V],
                                                       subscription: Subscription)
      extends KafkaSourceStage[K, V, TransactionalMessage[K, V]](
        s"TransactionalSource ${subscription.renderStageAttribute}"
      ) {
    require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty,
            "You must define a Consumer group.id.")

    /**
     * We set the isolation.level config to read_committed to make sure that any consumed messages are from
     * committed transactions. Note that the consuming partitions may be produced by multiple producers, and these
     * producers may either use transactional messaging or not at all. So the fetching partitions may have both
     * transactional and non-transactional messages, and by setting isolation.level config to read_committed consumers
     * will still consume non-transactional messages.
     */
    private val txConsumerSettings = consumerSettings.withProperty(
      ConsumerConfig.ISOLATION_LEVEL_CONFIG,
      IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH)
    )

    override protected def logic(shape: SourceShape[TransactionalMessage[K, V]]) =
      new SingleSourceLogic[K, V, TransactionalMessage[K, V]](shape, txConsumerSettings, subscription)
      with TransactionalMessageBuilder[K, V] {
        override def groupId: String = txConsumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)
      }
  }

  // This should be case class to be comparable based on ref and timeout. This comparison is used in CommittableOffsetBatchImpl
  case class KafkaAsyncConsumerCommitterRef(ref: ActorRef, commitTimeout: FiniteDuration)(implicit ec: ExecutionContext)
      extends Committer {
    import akka.pattern.ask

    import scala.collection.breakOut
    override def commit(offsets: immutable.Seq[PartitionOffsetMetadata]): Future[Done] = {
      val offsetsMap: Map[TopicPartition, OffsetAndMetadata] = offsets.map { offset =>
        new TopicPartition(offset.key.topic, offset.key.partition) ->
        new OffsetAndMetadata(offset.offset + 1, offset.metadata)
      }(breakOut)

      ref
        .ask(Commit(offsetsMap))(Timeout(commitTimeout))
        .map(_ => Done)
        .recoverWith {
          case _: AskTimeoutException =>
            Future.failed(new CommitTimeoutException(s"Kafka commit took longer than: $commitTimeout"))
          case other => Future.failed(other)
        }
    }

    override def commit(batch: CommittableOffsetBatch): Future[Done] = batch match {
      case b: CommittableOffsetBatchImpl =>
        val futures = b.offsetsAndMetadata.groupBy(_._1.groupId).map {
          case (groupId, offsetsMap) =>
            val committer = b.stages.getOrElse(
              groupId,
              throw new IllegalStateException(s"Unknown committer, got [$groupId]")
            )
            val offsets: immutable.Seq[PartitionOffsetMetadata] = offsetsMap.map {
              case (ctp, offset) => PartitionOffsetMetadata(ctp, offset.offset(), offset.metadata())
            }(breakOut)
            committer.commit(offsets)
        }
        Future.sequence(futures).map(_ => Done)

      case _ =>
        throw new IllegalArgumentException(
          s"Unknown CommittableOffsetBatch, got [${batch.getClass.getName}], " +
          s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
        )
    }
  }

  abstract class KafkaSourceStage[K, V, Msg](stageName: String)
      extends GraphStageWithMaterializedValue[SourceShape[Msg], Control] {
    protected val out = Outlet[Msg]("out")
    val shape = new SourceShape(out)

    override protected def initialAttributes: Attributes = Attributes.name(stageName)

    protected def logic(shape: SourceShape[Msg]): GraphStageLogic with Control
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
      val result = logic(shape)
      (result, result)
    }
  }

  private[kafka] trait PlainMessageBuilder[K, V] extends MessageBuilder[K, V, ConsumerRecord[K, V]] {
    override def createMessage(rec: ConsumerRecord[K, V]) = rec
  }

  private[kafka] trait TransactionalMessageBuilder[K, V] extends MessageBuilder[K, V, TransactionalMessage[K, V]] {
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

  private[kafka] trait CommittableMessageBuilder[K, V] extends MessageBuilder[K, V, CommittableMessage[K, V]] {
    def groupId: String
    def committer: Committer
    def metadataFromRecord(record: ConsumerRecord[K, V]): String

    override def createMessage(rec: ConsumerRecord[K, V]) = {
      val offset = ConsumerMessage.PartitionOffset(
        GroupTopicPartition(
          groupId = groupId,
          topic = rec.topic,
          partition = rec.partition
        ),
        offset = rec.offset
      )
      ConsumerMessage.CommittableMessage(rec, CommittableOffsetImpl(offset, metadataFromRecord(rec))(committer))
    }
  }

  final case class CommittableOffsetImpl(override val partitionOffset: ConsumerMessage.PartitionOffset,
                                         override val metadata: String)(
      val committer: Committer
  ) extends CommittableOffsetMetadata {
    override def commitScaladsl(): Future[Done] =
      committer.commit(immutable.Seq(partitionOffset.withMetadata(metadata)))
    override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava
  }

  trait Committer {
    // Commit all offsets (of different topics) belonging to the same stage
    def commit(offsets: immutable.Seq[PartitionOffsetMetadata]): Future[Done]
    def commit(batch: CommittableOffsetBatch): Future[Done]
  }

  final class CommittableOffsetBatchImpl(val offsetsAndMetadata: Map[GroupTopicPartition, OffsetAndMetadata],
                                         val stages: Map[String, Committer])
      extends CommittableOffsetBatch {
    def offsets = offsetsAndMetadata.mapValues(_.offset())

    def updated(committableOffset: CommittableOffset): CommittableOffsetBatch = {
      val partitionOffset = committableOffset.partitionOffset
      val key = partitionOffset.key
      val metadata = committableOffset match {
        case offset: CommittableOffsetMetadata =>
          offset.metadata
        case _ =>
          OffsetFetchResponse.NO_METADATA
      }

      val newOffsets =
        offsetsAndMetadata.updated(key, new OffsetAndMetadata(committableOffset.partitionOffset.offset, metadata))

      val stage = committableOffset match {
        case c: CommittableOffsetImpl => c.committer
        case _ =>
          throw new IllegalArgumentException(
            s"Unknown CommittableOffset, got [${committableOffset.getClass.getName}], " +
            s"expected [${classOf[CommittableOffsetImpl].getName}]"
          )
      }

      val newStages = stages.get(key.groupId) match {
        case Some(s) =>
          require(s == stage,
                  s"CommittableOffset [$committableOffset] origin stage must be same as other " +
                  s"stage with same groupId. Expected [$s], got [$stage]")
          stages
        case None =>
          stages.updated(key.groupId, stage)
      }

      new CommittableOffsetBatchImpl(newOffsets, newStages)
    }

    def updated(committableOffsetBatch: CommittableOffsetBatch): CommittableOffsetBatch =
      committableOffsetBatch match {
        case c: CommittableOffsetBatchImpl =>
          val newOffsetsAndMetadata = offsetsAndMetadata ++ c.offsetsAndMetadata
          val newStages = c.stages.foldLeft(stages) {
            case (acc, (groupId, stage)) =>
              acc.get(groupId) match {
                case Some(s) =>
                  require(
                    s == stage,
                    s"CommittableOffsetBatch [$committableOffsetBatch] origin stage for groupId [$groupId] " +
                    s"must be same as other stage with same groupId. Expected [$s], got [$stage]"
                  )
                  acc
                case None =>
                  acc.updated(groupId, stage)
              }
          }
          new CommittableOffsetBatchImpl(newOffsetsAndMetadata, newStages)
        case _ =>
          throw new IllegalArgumentException(
            s"Unknown CommittableOffsetBatch, got [${committableOffsetBatch.getClass.getName}], " +
            s"expected [${classOf[CommittableOffsetBatchImpl].getName}]"
          )
      }

    override def getOffsets(): JMap[GroupTopicPartition, Long] =
      offsets.asJava

    override def toString(): String =
      s"CommittableOffsetBatch(${offsets.mkString("->")})"

    override def commitScaladsl(): Future[Done] =
      if (offsets.isEmpty)
        Future.successful(Done)
      else {
        stages.head._2.commit(this)
      }

    override def commitJavadsl(): CompletionStage[Done] = commitScaladsl().toJava

  }

  final class WrappedConsumerControl(underlying: scaladsl.Consumer.Control) extends javadsl.Consumer.Control {
    override def stop(): CompletionStage[Done] = underlying.stop().toJava

    override def shutdown(): CompletionStage[Done] = underlying.shutdown().toJava

    override def drainAndShutdown[T](streamCompletion: CompletionStage[T], ec: Executor): CompletionStage[T] =
      underlying.drainAndShutdown(streamCompletion.toScala)(ExecutionContext.fromExecutor(ec)).toJava

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

  def onStop() =
    stopPromise.trySuccess(Done)

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

  protected def executionContext: ExecutionContext
  protected def consumerFuture: Future[ActorRef]

  def metrics: Future[Map[MetricName, Metric]] = {
    import akka.pattern.ask
    import scala.concurrent.duration._
    consumerFuture
      .flatMap { consumer =>
        consumer
          .ask(RequestMetrics)(Timeout(1.minute))
          .mapTo[ConsumerMetrics]
          .map(_.metrics)(ExecutionContexts.sameThreadExecutionContext)
      }(executionContext)
  }
}
