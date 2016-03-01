package com.softwaremill.react.kafka.commit

import akka.actor.Cancellable
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import com.softwaremill.react.kafka.commit.ConsumerCommitter.Contract.Flush
import com.typesafe.scalalogging.LazyLogging
import kafka.common.TopicAndPartition
import kafka.consumer.KafkaConsumer
import kafka.message.MessageAndMetadata

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success

class KafkaCommitterSink(committerFactory: CommitterProvider, kafkaConsumer: KafkaConsumer[_])
    extends GraphStage[SinkShape[KafkaMessage[_]]] with LazyLogging {

  val in: Inlet[KafkaMessage[_]] = Inlet("KafkaCommitterSink")
  val topic = kafkaConsumer.props.topic
  val DefaultCommitInterval = 10 seconds
  val commitInterval = kafkaConsumer.props.commitInterval.getOrElse(DefaultCommitInterval)
  var scheduledFlush: Option[Cancellable] = None
  var partitionOffsetMap = OffsetMap()
  var committedOffsetMap = OffsetMap()

  override val shape: SinkShape[KafkaMessage[_]] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    lazy val committerOpt: Option[OffsetCommitter] = createOffsetCommitter()

    implicit val timeout = Timeout(5 seconds)

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val msg = grab(in)
        registerCommit(msg)
        pull(in)
      }
    })

    override protected def onTimer(timerKey: Any): Unit = {
      if (timerKey == Flush)
        commitGatheredOffsets()
    }

    def registerCommit(msg: MessageAndMetadata[_, _]): Unit = {
      logger.debug(s"Received commit request for partition ${msg.partition} and offset ${msg.offset}")
      val topicPartition = TopicAndPartition(topic, msg.partition)
      val last = partitionOffsetMap.lastOffset(topicPartition)
      updateOffsetIfLarger(msg, last)
    }

    def updateOffsetIfLarger(msg: MessageAndMetadata[_, _], last: Long): Unit = {
      if (msg.offset > last) {
        logger.debug(s"Registering commit for partition ${msg.partition} and offset ${msg.offset}, last registered = $last")
        partitionOffsetMap = partitionOffsetMap.plusOffset(TopicAndPartition(topic, msg.partition), msg.offset)
        scheduleFlush()
      }
      else
        logger.debug(s"Skipping commit for partition ${msg.partition} and offset ${msg.offset}, last registered is $last")
    }

    def scheduleFlush(): Unit = {
      if (scheduledFlush.isEmpty) {
        scheduleOnce(Flush, commitInterval)
      }
    }

    def commitGatheredOffsets(): Unit = {
      logger.debug("Flushing offsets to commit")
      scheduledFlush = None
      committerOpt.foreach { committer =>
        val offsetMapToFlush = partitionOffsetMap.diff(committedOffsetMap)
        if (offsetMapToFlush.nonEmpty)
          performFlush(committer, offsetMapToFlush)
      }
    }

    def performFlush(committer: OffsetCommitter, offsetMapToFlush: OffsetMap): Unit = {
      val committedOffsetMapTry = committer.commit(offsetMapToFlush)
      committedOffsetMapTry match {
        case Success(resultOffsetMap) =>
          logger.debug(s"committed offsets: $resultOffsetMap")
          // We got the offset of the first unfetched message, and we want the
          // offset of the last fetched message
          committedOffsetMap = OffsetMap(committedOffsetMap.map ++ resultOffsetMap.map.mapValues(_ - 1))
        case scala.util.Failure(ex) =>
          logger.error("Failed to commit offsets", ex)
          committer.tryRestart() match {
            case scala.util.Failure(cause) =>
              logger.error("Fatal error, cannot re-establish connection. Stopping native committer.", cause)
              failStage(cause)
            case Success(()) =>
              logger.debug("Committer restarted. Rescheduling flush.")
              scheduleFlush()
          }
      }
    }

    def createOffsetCommitter() = {
      val factoryOrError = committerFactory.create(kafkaConsumer)
      factoryOrError.failed.foreach(err => logger.error(err.toString))
      factoryOrError.toOption
    }

    override def preStart(): Unit = {
      pull(in)
    }
  }
}
