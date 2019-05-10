/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import akka.NotUsed
import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{AutoSubscription, ConsumerSettings, ManualSubscription, Subscription}
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future

/** Internal API */
@InternalApi
private[kafka] final class PlainSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription)
    extends KafkaSourceStage[K, V, ConsumerRecord[K, V]](s"PlainSource ${subscription.renderStageAttribute}") {
  override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]): GraphStageLogic with Control =
    new SingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription) with PlainMessageBuilder[K, V]
}

/** Internal API */
@InternalApi
private[kafka] final class ExternalPlainSource[K, V](consumer: ActorRef, subscription: ManualSubscription)
    extends KafkaSourceStage[K, V, ConsumerRecord[K, V]](
      s"ExternalPlainSubSource ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]): GraphStageLogic with Control =
    new ExternalSingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, consumer, subscription)
    with PlainMessageBuilder[K, V] with MetricsControl
}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] final class PlainSubSource[K, V](
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription,
    getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]],
    onRevoke: Set[TopicPartition] => Unit
) extends KafkaSourceStage[K, V, (TopicPartition, Source[ConsumerRecord[K, V], NotUsed])](
      s"PlainSubSource ${subscription.renderStageAttribute}"
    ) {
  override protected def logic(
      shape: SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed])]
  ): GraphStageLogic with Control = {
    def factory(shape: SourceShape[ConsumerRecord[K, V]],
                tp: TopicPartition,
                consumerActor: ActorRef,
                subSourceStartedCb: AsyncCallback[(TopicPartition, (Control, ActorRef))],
                subSourceCancelledCb: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])],
                actorNumber: Int): MessageSubSourceLogic[K, V, ConsumerRecord[K, V]] =
      new MessageSubSourceLogic[K, V, ConsumerRecord[K, V]](shape,
                                                            tp,
                                                            consumerActor,
                                                            subSourceStartedCb,
                                                            subSourceCancelledCb,
                                                            actorNumber) with PlainMessageBuilder[K, V]
    new SubSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription, factory, getOffsetsOnAssign, onRevoke)
  }
}
