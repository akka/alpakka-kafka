/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.NotUsed
import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.kafka.Subscriptions.{TopicSubscription, TopicSubscriptionPattern}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{AutoSubscription, ConsumerSettings, KafkaConsumerActor}
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.stream.{ActorMaterializerHelper, Attributes, Outlet, SourceShape}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.{immutable, mutable}

private[kafka] abstract class SubSourceLogic[K, V, Msg](
    val shape: SourceShape[(TopicPartition, Source[Msg, NotUsed])],
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription
) extends GraphStageLogic(shape) with PromiseControl with MessageBuilder[K, V, Msg] {
  val partitionAssignedCB = getAsyncCallback[Iterable[TopicPartition]] { tps =>
    buffer = buffer.enqueue(tps.toList)
    pump()
  }
  val partitionRevokedCB = getAsyncCallback[Iterable[TopicPartition]] { r =>
    r.map(subSources.get).foreach(_.foreach(_.shutdown()))
    subSources --= r
  }
  val subsourceCancelledCB = getAsyncCallback[TopicPartition] { tp =>
    subSources -= tp
    buffer :+= tp
    pump()
  }
  val subsourceStartedCB = getAsyncCallback[(TopicPartition, Control)] {
    case (tp, control) => subSources += (tp -> control)
  }
  var consumer: ActorRef = _
  var self: StageActor = _
  var buffer: immutable.Queue[TopicPartition] = immutable.Queue.empty
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
        failStage(new Exception("Consumer actor terminated"))
    }
    self.watch(consumer)

    def rebalanceListener =
      KafkaConsumerActor.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke)

    subscription match {
      case TopicSubscription(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener), self.ref)
      case TopicSubscriptionPattern(topics) =>
        consumer.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener), self.ref)
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

  @tailrec
  private def pump(): Unit = {
    if (buffer.nonEmpty && isAvailable(shape.out)) {
      val (tp, remains) = buffer.dequeue
      buffer = remains
      push(shape.out, (tp, createSource(tp)))
      pump()
    }
  }

  class SubSourceStage(tp: TopicPartition, consumer: ActorRef) extends GraphStage[SourceShape[Msg]] {
    stage =>
    val out = Outlet[Msg]("out")
    val shape = new SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with PromiseControl {
        val shape = stage.shape
        val requestMessages = KafkaConsumerActor.Internal.RequestMessages(0, Set(tp))
        val pauseMessages = KafkaConsumerActor.Internal.PauseMessages(0, Set(tp))
        val unpauseMessages = KafkaConsumerActor.Internal.UnpauseMessages(0, Set(tp))

        var paused = false
        var self: StageActor = _
        var buffer: mutable.Queue[ConsumerRecord[K, V]] = mutable.Queue()

        val bufferSize = settings.sourceBufferSize

        def checkBufferAndPause(): Unit =
          if (!paused && buffer.size >= bufferSize) {
            consumer.tell(pauseMessages, self.ref)
            paused = true
          }

        def checkBufferAndUnpause(): Unit =
          if (paused && buffer.size < bufferSize) {
            consumer.tell(unpauseMessages, self.ref)
            paused = false
          }

        override def preStart(): Unit = {
          subsourceStartedCB.invoke((tp, this))
          self = getStageActor {
            case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
              //              requested = false

              buffer.enqueue(msg.messages.toSeq: _*)
              checkBufferAndPause()
              pump()
            case (_, Terminated(ref)) if ref == consumer =>
              failStage(new Exception("Consumer actor terminated"))
          }
          consumer.tell(requestMessages, self.ref)
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
          if (isAvailable(out) && buffer.nonEmpty) {
            val msg = buffer.dequeue()
            push(out, createMessage(msg))
            checkBufferAndUnpause()
            pump()
          }
        }
      }
    }
  }
}
