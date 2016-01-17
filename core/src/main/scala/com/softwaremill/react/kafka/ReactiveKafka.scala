package com.softwaremill.react.kafka

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.stream.SourceShape
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.GraphStage
import com.softwaremill.react.kafka.ReactiveKafka.DefaultRequestStrategy
import com.softwaremill.react.kafka.commit.{CommitSink, KafkaCommitterSink, KafkaSink, OffsetMap}
import kafka.producer._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.{Publisher, Subscriber}
import ReactiveKafkaConsumer.{NoPartitionSpecified, NoOffsetSpecified}

class ReactiveKafka {

  def publish[K, V](
    props: ProducerProperties[K, V],
    requestStrategy: () => RequestStrategy
  )(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage[K, V]] = {
    ActorSubscriber[ProducerMessage[K, V]](producerActor(props, requestStrategy))
  }

  def publish[K, V](
    props: ProducerProperties[K, V],
    requestStrategy: () => RequestStrategy,
    dispatcher: String
  )(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage[K, V]] = {
    ActorSubscriber[ProducerMessage[K, V]](producerActor(props, requestStrategy, dispatcher))
  }

  def publish[K, V](
    props: ProducerProperties[K, V],
    dispatcher: String
  )(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage[K, V]] = {
    ActorSubscriber[ProducerMessage[K, V]](producerActor(props, dispatcher))
  }

  def publish[K, V](
    props: ProducerProperties[K, V]
  )(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage[K, V]] = {
    ActorSubscriber[ProducerMessage[K, V]](producerActor(props))
  }

  def producerActor[K, V](
    props: ProducerProperties[K, V],
    requestStrategy: () => RequestStrategy
  )(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, requestStrategy, "kafka-subscriber-dispatcher")
  }

  def producerActor[K, V](
    props: ProducerProperties[K, V],
    dispatcher: String
  )(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, DefaultRequestStrategy, dispatcher)
  }

  def producerActor[K, V](
    props: ProducerProperties[K, V],
    requestStrategy: () => RequestStrategy,
    dispatcher: String
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(producerActorProps(props, requestStrategy).withDispatcher(dispatcher))
  }

  def producerActorProps[K, V](
    props: ProducerProperties[K, V],
    requestStrategy: () => RequestStrategy
  ) = {
    val producer = new ReactiveKafkaProducer(props)
    Props(
      new KafkaActorSubscriber[K, V](producer, requestStrategy)
    )
  }

  def producerActorProps[K, V](props: ProducerProperties[K, V]): Props = {
    producerActorProps(props, DefaultRequestStrategy)
  }

  def producerActor[K, V](
    props: ProducerProperties[K, V]
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(producerActorProps(props))
  }

  def consume[K, V](
    props: ConsumerProperties[K, V]
  )(implicit actorSystem: ActorSystem) = {
    ActorPublisher[ConsumerRecord[K, V]](consumerActor(props))
  }

  def graphStageSink[K, V](props: ProducerProperties[K, V]) = {
    val producer = new ReactiveKafkaProducer(props)
    Sink.fromGraph(new KafkaGraphStageSink(producer))
  }

  def graphStageSource[K, V](
    props: ConsumerProperties[K, V],
    partition: Int = NoPartitionSpecified,
    offset: Long = NoOffsetSpecified
  ) = {
    val consumer = ReactiveKafkaConsumer(props, partition, offset)
    Source.fromGraph(new KafkaGraphStageSource(consumer))
  }

  def consumeWithOffsetSink[K, V](
    props: ConsumerProperties[K, V]
  )(implicit actorSystem: ActorSystem): PublisherWithCommitSink[K, V] = {
    val actorWithConsumer = consumerActorWithConsumer(props.noAutoCommit(), ReactiveKafka.ConsumerDefaultDispatcher)
    PublisherWithCommitSink[K, V](
      ActorPublisher[ConsumerRecord[K, V]](
        actorWithConsumer.actor
      ),
      actorWithConsumer.actor,
      CommitSink.create(actorWithConsumer.actor, props)
    )
  }

  def sourceWithOffsetSink[K, V](
    props: ConsumerProperties[K, V],
    partition: Int = NoPartitionSpecified,
    offset: Long = NoOffsetSpecified
  ): SourceWithCommitSink[K, V] = {
    val offsetMap = OffsetMap()
    val finalProperties: ConsumerProperties[K, V] = props.noAutoCommit()
    val offsetSink = Sink.fromGraph(new KafkaCommitterSink(finalProperties, offsetMap))
    val consumer: ReactiveKafkaConsumer[K, V] = new ReactiveKafkaConsumer(finalProperties, partition, offset)
    val source = Source.fromGraph(new KafkaGraphStageSource(consumer, offsetMap))
    SourceWithCommitSink(source, offsetSink, consumer)
  }

  def consume[K, V](
    props: ConsumerProperties[K, V],
    dispatcher: String
  )(implicit actorSystem: ActorSystem) = {
    ActorPublisher[ConsumerRecord[K, V]](consumerActor(props, dispatcher))
  }

  def consumerActor[K, V](props: ConsumerProperties[K, V])(implicit actorSystem: ActorSystem): ActorRef = {
    consumerActor(props, ReactiveKafka.ConsumerDefaultDispatcher)
  }

  def consumerActor[K, V](
    props: ConsumerProperties[K, V],
    dispatcher: String
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(consumerActorProps(props).withDispatcher(dispatcher))
  }

  private def consumerActorWithConsumer[K, V](
    props: ConsumerProperties[K, V],
    dispatcher: String
  )(implicit actorSystem: ActorSystem) = {
    val propsWithConsumer = consumerActorPropsWithConsumer(props)
    val actor = actorSystem.actorOf(propsWithConsumer.actorProps.withDispatcher(dispatcher))
    ConsumerWithActor(propsWithConsumer.consumer, actor)
  }

  private def consumerActorPropsWithConsumer[K, V](
    props: ConsumerProperties[K, V],
    partition: Int = NoPartitionSpecified,
    offset: Long = NoOffsetSpecified
  ) = {
    val reactiveConsumer = ReactiveKafkaConsumer(props, partition, offset)
    ConsumerWithActorProps(reactiveConsumer, Props(new KafkaActorPublisher(reactiveConsumer)))
  }

  def consumerActorProps[K, V](
    props: ConsumerProperties[K, V],
    partition: Int = NoPartitionSpecified,
    offset: Long = NoOffsetSpecified
  ) = {
    val reactiveConsumer = ReactiveKafkaConsumer(props, partition, offset)
    Props(new KafkaActorPublisher(reactiveConsumer))
  }
}

object ReactiveKafka {
  val DefaultRequestStrategy = () => WatermarkRequestStrategy(10)
  val ConsumerDefaultDispatcher = "kafka-publisher-dispatcher"
}

case class SourceWithCommitSink[K, V](
  source: Source[ConsumerRecord[K, V], Unit],
  offsetCommitSink: Sink[ConsumerRecord[K, V], Unit],
  underlyingConsumer: ReactiveKafkaConsumer[K, V]
)

case class PublisherWithCommitSink[K, V](
    publisher: Publisher[ConsumerRecord[K, V]],
    publisherActor: ActorRef,
    kafkaOffsetCommitSink: KafkaSink[ConsumerRecord[K, V]]
) {
  def offsetCommitSink = kafkaOffsetCommitSink.sink

  def cancel(): Unit = {
    publisherActor ! Cancel
    kafkaOffsetCommitSink.underlyingCommitterActor ! PoisonPill
  }
}

case class GraphStageSourceWithCommitSink[K, V](
    sourceStage: GraphStage[SourceShape[ConsumerRecord[K, V]]],
    kafkaOffsetCommitSink: KafkaSink[ConsumerRecord[K, V]]
) {
  def offsetCommitSink = kafkaOffsetCommitSink.sink
}

private[kafka] case class ConsumerWithActorProps[K, V](consumer: ReactiveKafkaConsumer[K, V], actorProps: Props)
private[kafka] case class ConsumerWithActor[K, V](consumer: ReactiveKafkaConsumer[K, V], actor: ActorRef)