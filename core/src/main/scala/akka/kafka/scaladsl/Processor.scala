package akka.kafka.scaladsl

import akka.kafka.ProducerMessage.Envelope
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, ProducerSettings, Subscription}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Source}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

object Processor {

  def atLeastOnceSource[K, V](
      consumerSettings: ConsumerSettings[K, V],
      subscription: Subscription,
      committerSettings: CommitterSettings
  )(
      process: ConsumerRecord[K, V] => Future[Done]
  )(implicit materializer: Materializer): Source[ConsumerMessage.CommittableOffsetBatch, Consumer.Control] = {
    Consumer
      .sourceWithOffsetContext(consumerSettings, subscription)
      .mapAsync(1)(process)
      .viaMat(Committer.flowWithOffsetContext(committerSettings))(Keep.left)
      .asSource
      .map(_._2)
  }

  def atLeastOnceRunner[K, V](
      consumerSettings: ConsumerSettings[K, V],
      subscription: Subscription,
      committerSettings: CommitterSettings
  )(process: ConsumerRecord[K, V] => Future[Done])(implicit materializer: Materializer): DrainingControl[Done] = {
    Consumer
      .sourceWithOffsetContext(consumerSettings, subscription)
      .mapAsync(1)(process)
      .toMat(Committer.sinkWithOffsetContext(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

  def consumeAndProduceSource[CK, CV, PK, PV](
      consumerSettings: ConsumerSettings[CK, CV],
      subscription: Subscription,
      producerSettings: ProducerSettings[PK, PV],
      committerSettings: CommitterSettings
  )(
      process: ConsumerRecord[CK, CV] => Future[Envelope[PK, PV, NotUsed]]
  )(implicit materializer: Materializer): Source[ConsumerMessage.CommittableOffsetBatch, Consumer.Control] = {
    Consumer
      .sourceWithOffsetContext(consumerSettings, subscription)
      .mapAsync(1)(process)
      .viaMat(Producer.flowWithContext(producerSettings))(Keep.left)
      .viaMat(Committer.flowWithOffsetContext(committerSettings))(Keep.left)
      .asSource
      .map(_._2)
  }

  def consumeAndProduceRunner[CK, CV, PK, PV](
      consumerSettings: ConsumerSettings[CK, CV],
      subscription: Subscription,
      producerSettings: ProducerSettings[PK, PV],
      committerSettings: CommitterSettings
  )(
      process: ConsumerRecord[CK, CV] => Future[Envelope[PK, PV, NotUsed]]
  )(implicit materializer: Materializer): DrainingControl[Done] = {
    Consumer
      .sourceWithOffsetContext(consumerSettings, subscription)
      .mapAsync(1)(process)
      .toMat(Producer.committableSinkWithOffsetContext(producerSettings, committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

}
