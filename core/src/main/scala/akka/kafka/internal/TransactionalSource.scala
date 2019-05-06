/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.Locale

import akka.Done
import akka.actor.{ActorRef, Status, Terminated}
import akka.actor.Status.Failure
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.TransactionalMessage
import akka.kafka.internal.KafkaConsumerActor.Internal.Revoked
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerFailed, ConsumerSettings, Subscription}
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic
import akka.util.Timeout
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.IsolationLevel

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

/** Internal API */
@InternalApi
private[kafka] final class TransactionalSource[K, V](consumerSettings: ConsumerSettings[K, V],
                                                     subscription: Subscription)
    extends KafkaSourceStage[K, V, TransactionalMessage[K, V]](
      s"TransactionalSource ${subscription.renderStageAttribute}"
    ) {
  import TransactionalSource._

  require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty, "You must define a Consumer group.id.")

  type Offset = Long

  private trait InFlightRecords {
    // Assumes that offsets per topic partition are added in the increasing order
    // The assumption is true for Kafka consumer that guarantees that elements are emitted
    // per partition in offset-increasing order.
    def add(offsets: Map[TopicPartition, Offset]): Unit
    def committed(offsets: Map[TopicPartition, Offset]): Unit
    def revoke(revokedTps: Set[TopicPartition]): Unit
    def reset(): Unit
    def assigned(): Set[TopicPartition]

    def empty(partitions: Set[TopicPartition]): Boolean
  }

  private object InFlightRecords {
    def empty = new Impl

    class Impl extends InFlightRecords {
      private var inFlightRecords: Map[TopicPartition, Offset] = Map.empty

      override def add(offsets: Map[TopicPartition, Offset]): Unit =
        inFlightRecords = inFlightRecords ++ offsets

      override def committed(committed: Map[TopicPartition, Offset]): Unit =
        inFlightRecords = inFlightRecords.flatMap {
          case (tp, offset) if committed.get(tp).contains(offset) => None
          case x => Some(x)
        }

      override def revoke(revokedTps: Set[TopicPartition]): Unit =
        inFlightRecords = inFlightRecords -- revokedTps

      override def reset(): Unit = inFlightRecords = Map.empty

      override def empty(partitions: Set[TopicPartition]): Boolean = partitions.flatMap(inFlightRecords.get(_)).isEmpty

      override def toString: String = inFlightRecords.toString()

      override def assigned(): Set[TopicPartition] = inFlightRecords.keySet
    }
  }

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

  override protected def logic(shape: SourceShape[TransactionalMessage[K, V]]): GraphStageLogic with Control =
    new SingleSourceLogic[K, V, TransactionalMessage[K, V]](shape, txConsumerSettings, subscription)
    with TransactionalMessageBuilder[K, V] {
      val inFlightRecords = InFlightRecords.empty

      override def messageHandling = super.messageHandling.orElse(drainHandling).orElse {
        case (_, Revoked(tps)) =>
          inFlightRecords.revoke(tps.toSet)
      }

      override def shuttingDownReceive =
        super.shuttingDownReceive
          .orElse(drainHandling)
          .orElse {
            case (_, Status.Failure(e)) =>
              failStage(e)
            case (_, Terminated(ref)) if ref == consumerActor =>
              failStage(new ConsumerFailed())
          }

      def drainHandling: PartialFunction[(ActorRef, Any), Unit] = {
        case (sender, Committed(offsets)) =>
          inFlightRecords.committed(offsets.mapValues(_.offset() - 1))
          sender ! Done
        case (sender, CommittingFailure) => {
          log.info("Committing failed, resetting in flight offsets")
          inFlightRecords.reset()
        }
        case (sender, Drain(partitions, ack, msg)) =>
          if (inFlightRecords.empty(partitions)) {
            log.debug(s"Partitions drained ${partitions.mkString(",")}")
            ack.getOrElse(sender) ! msg
          } else {
            log.debug(s"Draining partitions {}", partitions)
            materializer.scheduleOnce(consumerSettings.drainingCheckInterval, new Runnable {
              override def run(): Unit =
                sourceActor.ref ! Drain(partitions, ack.orElse(Some(sender)), msg)
            })
          }
      }

      override def groupId: String = txConsumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)
      lazy val committedMarker: CommittedMarker = {
        val ec = materializer.executionContext
        CommittedMarkerRef(sourceActor.ref, consumerSettings.commitTimeout)(ec)
      }

      override def onMessage(rec: ConsumerRecord[K, V]): Unit =
        inFlightRecords.add(Map(new TopicPartition(rec.topic(), rec.partition()) -> rec.offset()))

      override protected def stopConsumerActor(): Unit =
        sourceActor.ref.tell(Drain(inFlightRecords.assigned(), Some(consumerActor), KafkaConsumerActor.Internal.Stop),
                             sourceActor.ref)

      // This is invoked in the KafkaConsumerActor thread when doing poll.
      override def blockingRevokedHandler(revokedTps: Set[TopicPartition]): Unit =
        if (waitForDraining(revokedTps)) {
          sourceActor.ref ! Revoked(revokedTps.toList)
        } else {
          sourceActor.ref ! Failure(new Error("Timeout while draining"))
          consumerActor ! KafkaConsumerActor.Internal.Stop
        }

      def waitForDraining(partitions: Set[TopicPartition]): Boolean = {
        import akka.pattern.ask
        implicit val timeout = Timeout(consumerSettings.commitTimeout)
        try {
          Await.result(ask(stageActor.ref, Drain(partitions, None, Drained)), timeout.duration)
          true
        } catch {
          case t: Throwable =>
            false
        }
      }
    }

}

/** Internal API */
@InternalApi
private object TransactionalSource {
  case object Drained
  case class Drain[T](partitions: Set[TopicPartition],
                      drainedConfirmationRef: Option[ActorRef],
                      drainedConfirmationMsg: T)
  case class Committed(offsets: Map[TopicPartition, OffsetAndMetadata])
  case object CommittingFailure

  private[kafka] final case class CommittedMarkerRef(sourceActor: ActorRef, commitTimeout: FiniteDuration)(
      implicit ec: ExecutionContext
  ) extends CommittedMarker {
    override def committed(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Done] = {
      import akka.pattern.ask
      sourceActor
        .ask(Committed(offsets))(Timeout(commitTimeout))
        .map(_ => Done)
    }

    override def failed(): Unit =
      sourceActor ! CommittingFailure
  }
}
