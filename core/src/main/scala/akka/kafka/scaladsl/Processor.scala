package akka.kafka.scaladsl

import akka.kafka.ProducerMessage.Envelope
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings, Subscription}
import akka.stream.Materializer
import akka.stream.scaladsl.Keep
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

object Processor {

  def atLeastOnce[K, V](
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

  def consumeAndProduce[CK, CV, PK, PV](
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
