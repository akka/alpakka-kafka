/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.kafka.Subscriptions.{TopicSubscription, TopicSubscriptionPattern}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{AutoSubscription, ConsumerActorTerminatedException, ConsumerSettings, KafkaConsumerActor}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.stream.{ActorMaterializerHelper, Attributes, Outlet, SourceShape}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Future

private[kafka] abstract class SubSourceLogic[K, V, Msg](
    val shape: SourceShape[(TopicPartition, Source[Msg, NotUsed])],
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription,
    loadOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]] = None,
    onRevoke: Set[TopicPartition] => Unit = _ => ()
) extends GraphStageLogic(shape) with PromiseControl with MessageBuilder[K, V, Msg] {
  var consumer: ActorRef = _
  var self: StageActor = _
  // Kafka has notified us that we have these partitions assigned, but we have not created a source for them yet.
  var pendingPartitions: immutable.Set[TopicPartition] = immutable.Set.empty
  // We have created a source for these partitions, but it has not started up and is not in subSources yet.
  var partitionsInStartup: immutable.Set[TopicPartition] = immutable.Set.empty
  var subSources: Map[TopicPartition, Control] = immutable.Map.empty

  override def preStart(): Unit = {
    super.preStart()
    consumer = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      val name = s"kafka-consumer-${KafkaConsumerActor.Internal.nextNumber()}"
      extendedActorSystem.systemActorOf(KafkaConsumerActor.props(settings), name)
    }

    self = getStageActor {
      case (_, Terminated(ref)) if ref == consumer =>
        failStage(new ConsumerActorTerminatedException)
    }
    self.watch(consumer)

    def rebalanceListener =
      KafkaConsumerActor.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke)

    subscription match {
      case TopicSubscription(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener, loadOffsetsOnAssign.isEmpty), self.ref)
      case TopicSubscriptionPattern(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener, loadOffsetsOnAssign.isEmpty), self.ref)
    }
  }

  implicit val seekTimeout = Timeout(5000, TimeUnit.MILLISECONDS) // TODO: configurable? retry?
  val partitionAssignedCB = getAsyncCallback[Iterable[TopicPartition]] { tps =>
    pendingPartitions ++= tps.filter(!partitionsInStartup.contains(_))
    loadOffsetsOnAssign.foreach { loadOffsets =>
      implicit val ec = materializer.executionContext
      loadOffsets(tps.toSet)
        .flatMap { offsets => consumer.ask(KafkaConsumerActor.Internal.Seek(offsets)) }
        .foreach(_ => pump())
    }
  }

  val partitionRevokedCB = getAsyncCallback[Iterable[TopicPartition]] { tps =>
    onRevoke(tps.toSet)
    pendingPartitions --= tps
    partitionsInStartup --= tps
    tps.flatMap(subSources.get).foreach(_.shutdown())
    subSources --= tps
  }

  val subsourceCancelledCB = getAsyncCallback[TopicPartition] { tp =>
    subSources -= tp
    partitionsInStartup -= tp
    pendingPartitions += tp
    pump()
  }

  val subsourceStartedCB = getAsyncCallback[(TopicPartition, Control)] {
    case (tp, control) =>
      if (!partitionsInStartup.contains(tp)) {
        // Partition was revoked while
        // starting up.  Kill!
        control.shutdown()
      }
      else {
        subSources += (tp -> control)
        partitionsInStartup -= tp
      }
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = {
      pump()
    }
    override def onDownstreamFinish(): Unit = {
      performShutdown()
    }
  })

  def createSource(tp: TopicPartition): Source[Msg, NotUsed] = {
    Source.fromGraph(new SubSourceStage(tp, consumer))
  }

  @tailrec
  private def pump(): Unit = {
    if (pendingPartitions.nonEmpty && isAvailable(shape.out)) {
      val tp = pendingPartitions.head

      pendingPartitions = pendingPartitions.tail
      partitionsInStartup += tp
      push(shape.out, (tp, createSource(tp)))
      pump()
    }
  }

  override def postStop(): Unit = {
    consumer ! KafkaConsumerActor.Internal.Stop
    onShutdown()
    super.postStop()
  }

  override def performShutdown() = {
    setKeepGoing(true)
    //todo we should wait for subsources to be shutdown and next shutdown main stage
    subSources.foreach {
      case (_, control) => control.shutdown()
    }

    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    self.become {
      case (_, Terminated(ref)) if ref == consumer =>
        onShutdown()
        completeStage()
    }
    consumer ! KafkaConsumerActor.Internal.Stop
  }

  class SubSourceStage(tp: TopicPartition, consumer: ActorRef) extends GraphStage[SourceShape[Msg]] { stage =>
    val out = Outlet[Msg]("out")
    val shape = new SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with PromiseControl {
        val shape = stage.shape
        val requestMessages = KafkaConsumerActor.Internal.RequestMessages(0, Set(tp))
        var requested = false
        var self: StageActor = _
        var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

        override def preStart(): Unit = {
          super.preStart()
          subsourceStartedCB.invoke((tp, this))
          self = getStageActor {
            case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
              requested = false
              // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
              if (buffer.hasNext) {
                buffer = buffer ++ msg.messages
              }
              else {
                buffer = msg.messages
              }
              pump()
            case (_, Terminated(ref)) if ref == consumer =>
              failStage(new ConsumerActorTerminatedException)
          }
          self.watch(consumer)
        }

        override def postStop(): Unit = {
          onShutdown()
          super.postStop()
        }

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pump()
          }

          override def onDownstreamFinish(): Unit = {
            subsourceCancelledCB.invoke(tp)
            super.onDownstreamFinish()
          }
        })

        def performShutdown() = {
          completeStage()
        }

        @tailrec
        private def pump(): Unit = {
          if (isAvailable(out)) {
            if (buffer.hasNext) {
              val msg = buffer.next()
              push(out, createMessage(msg))
              pump()
            }
            else if (!requested) {
              requested = true
              consumer.tell(requestMessages, self.ref)
            }
          }
        }
      }
    }
  }
}
