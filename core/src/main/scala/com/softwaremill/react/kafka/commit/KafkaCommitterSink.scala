package com.softwaremill.react.kafka.commit

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.softwaremill.react.kafka.ConsumerProperties
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

class KafkaCommitterSink(consumerProps: ConsumerProperties[_, _], partitionOffsetMap: OffsetMap)
    extends GraphStage[SinkShape[ConsumerRecord[_, _]]] with LazyLogging {

  val in: Inlet[ConsumerRecord[_, _]] = Inlet("KafkaCommitterSink")
  val topic = consumerProps.topic

  override val shape: SinkShape[ConsumerRecord[_, _]] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val consumerRecord = grab(in)
        val topicPartition = new TopicPartition(topic, consumerRecord.partition)
        val last = partitionOffsetMap.lastOffset(topicPartition)
        updateOffsetIfLarger(consumerRecord, last)
        pull(in)
      }

      def updateOffsetIfLarger(msg: ConsumerRecord[_, _], last: Long): Unit = {
        if (msg.offset > last) {
          partitionOffsetMap.updateWithOffset(new TopicPartition(topic, msg.partition), msg.offset)
        }
        else
          logger.debug(s"Skipping commit for partition ${msg.partition} and offset ${msg.offset}, last registered is $last")
      }
    })

    override def preStart(): Unit = {
      pull(in)
    }
  }
}