package sample.scaladsl

import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.ProducerRecord

// Connect a Consumer to Producer, mapping messages one-to-many, and commit in batches
object AtLeastOnceOneToManyExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    val done =
      // #oneToMany
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapConcat(msg =>
          List(
            ProducerMessage.Message(
              new ProducerRecord[Array[Byte], String]("topic2", msg.record.value),
              None
            ),
            ProducerMessage.Message(
              new ProducerRecord[Array[Byte], String]("topic2", msg.record.value),
              Some(msg.committableOffset)
            )
          ))
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        .collect{ case Some(offset) => offset }
        .batch(max = 20, CommittableOffsetBatch.empty.updated(_))(_.updated(_))
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #oneToMany

    terminateWhenDone(done)
  }
}
