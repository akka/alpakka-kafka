package com.softwaremill.react.kafka

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.ReactiveKafka.DefaultRequestStrategy
import com.softwaremill.react.kafka.commit.{CommitSink, KafkaCommitterSink, KafkaSink, OffsetMap}
import kafka.producer._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.reactivestreams.{Publisher, Subscriber}

import scalaj.collection.Imports._

class ReactiveKafka {
  def graphStageSink[K, V](props: ProducerProperties[K, V]) = {
    val producer = new ReactiveKafkaProducer(props)
    Sink.fromGraph(new KafkaGraphStageSink(producer))
  }

  // Scala DSL - props
  def graphStageSource[K, V](
    props: ConsumerProperties[K, V]
  ) = {
    val consumer = ReactiveKafkaConsumer(props)
    Source.fromGraph(new KafkaGraphStageSource(consumer))
  }

  // Scala DSL - props and topicsAndPartitions
  def graphStageSource[K, V](
    props: ConsumerProperties[K, V],
    topicsAndPartitions: Set[TopicPartition]
  ) = {
    val consumer = ReactiveKafkaConsumer(props, topicsAndPartitions)
    Source.fromGraph(new KafkaGraphStageSource(consumer))
  }

  // Scala DSL - props and topicPartitionOffsetsMap
  def graphStageSource[K, V](
    props: ConsumerProperties[K, V],
    topicPartitionOffsetsMap: Map[TopicPartition, Long]
  ) = {
    val consumer = ReactiveKafkaConsumer(props, Set(), topicPartitionOffsetsMap)
    Source.fromGraph(new KafkaGraphStageSource(consumer))
  }

  // Java DSL - props
  def graphStageJavaSource[K, V](props: ConsumerProperties[K, V]) = {
    val consumer = ReactiveKafkaConsumer(props)
    akka.stream.javadsl.Source.fromGraph(new KafkaGraphStageSource(consumer))
  }

  // Java DSL - props and topicsAndPartitions
  def graphStageJavaSource[K, V](
    props: ConsumerProperties[K, V],
    topicsAndPartitions: java.util.Set[TopicPartition]
  ) = {
    val consumer = ReactiveKafkaConsumer(props, topicsAndPartitions)
    akka.stream.javadsl.Source.fromGraph(new KafkaGraphStageSource(consumer))
  }

  // Java DSL - props and topicPartitionOffsetMap
  def graphStageJavaSource[K, V](
    props: ConsumerProperties[K, V],
    topicPartitionOffsetsMap: java.util.Map[TopicPartition, java.lang.Long]
  ) = {
    val consumer = ReactiveKafkaConsumer(
      props,
      topicPartitionOffsetsMap
    )
    akka.stream.javadsl.Source.fromGraph(new KafkaGraphStageSource(consumer))
  }


  // Scala DSL
  def sourceWithOffsetSink[K, V](
    props: ConsumerProperties[K, V],
    topicsAndPartitions: Set[TopicPartition] = Set(),
    topicPartitionOffsetsMap: Map[TopicPartition, Long] = Map()
  ): SourceWithCommitSink[K, V] = {
    val offsetMap = OffsetMap()
    val finalProperties: ConsumerProperties[K, V] = props.noAutoCommit()
    val offsetSink = Sink.fromGraph(new KafkaCommitterSink(finalProperties, offsetMap))
    val consumer: ReactiveKafkaConsumer[K, V] = new ReactiveKafkaConsumer(finalProperties, topicsAndPartitions, topicPartitionOffsetsMap)
    val source = Source.fromGraph(new KafkaGraphStageSource(consumer, offsetMap))
    SourceWithCommitSink(source, offsetSink, consumer)
  }

  // Java DSL - props
  def sourceWithOffsetSink[K, V](
    props: ConsumerProperties[K, V]
  ): SourceWithCommitSink[K, V] = {
    sourceWithOffsetSink(props, Set(), Map())
  }

  // Java DSL - props and partitionsAndTopics
  def sourceWithOffsetSink[K, V](
    props: ConsumerProperties[K, V],
    topicsAndPartitions: java.util.Set[TopicPartition]
  ): SourceWithCommitSink[K, V] = {
    sourceWithOffsetSink(props, topicsAndPartitions.asScala.toSet, Map())
  }

  // Java DSL - props and topicPartitionOffsetsMap
  def sourceWithOffsetSink[K, V](
    props: ConsumerProperties[K, V],
    topicPartitionOffsetsMap: java.util.Map[TopicPartition, java.lang.Long]
  ): SourceWithCommitSink[K, V] = {
    sourceWithOffsetSink(props, Set(), topicPartitionOffsetsMap.asScala.toMap)
  }
}

object ReactiveKafka {
  val DefaultRequestStrategy = () => WatermarkRequestStrategy(10)
  val ConsumerDefaultDispatcher = "kafka-publisher-dispatcher"
}

case class SourceWithCommitSink[K, V](
  source: Source[ConsumerRecord[K, V], NotUsed],
  offsetCommitSink: Sink[ConsumerRecord[K, V], NotUsed],
  underlyingConsumer: ReactiveKafkaConsumer[K, V]
)

private[kafka] case class ConsumerWithActorProps[K, V](consumer: ReactiveKafkaConsumer[K, V], actorProps: Props)
private[kafka] case class ConsumerWithActor[K, V](consumer: ReactiveKafkaConsumer[K, V], actor: ActorRef)

sealed trait ProducerMessage[K, V] {
  def key: K
  def value: V
}

case class KeyValueProducerMessage[K, V](key: K, value: V) extends ProducerMessage[K, V]

case class ValueProducerMessage[V](value: V) extends ProducerMessage[Array[Byte], V] {
  override def key: Array[Byte] = null
}

object ProducerMessage {
  def apply[K, V](consumerRecord: ConsumerRecord[K, V]): ProducerMessage[K, V] =
    new KeyValueProducerMessage(consumerRecord.key(), consumerRecord.value())

  def apply[K, V](k: K, v: V): ProducerMessage[K, V] =
    new KeyValueProducerMessage(k, v)

  def apply[V](v: V): ProducerMessage[Array[Byte], V] = {
    new ValueProducerMessage(v)
  }
}
