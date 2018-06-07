package sample.scaladsl
// #oneToMany
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.ProducerMessage.{Message, Envelope, MultiMessage, PassThroughMessage}
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable

// #oneToMany

// Connect a Consumer to Producer, mapping messages one-to-many, and commit in batches
object AtLeastOnceOneToManyExample extends ConsumerExample {

  def main(args: Array[String]): Unit = {
    val done =
      // #oneToMany
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .map(msg =>
          MultiMessage(
            immutable.Seq(
              new ProducerRecord("topic2", msg.record.key, msg.record.value),
              new ProducerRecord("topic3", msg.record.key, msg.record.value)
            ),
            msg.committableOffset
          )
        )
        .via(Producer.flow2(producerSettings))
        .map(_.passThrough)
        .batch(max = 20, CommittableOffsetBatch(_))(_.updated(_))
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #oneToMany

    terminateWhenDone(done)
  }
}

object AtLeastOnceOneToConditionalExample extends ConsumerExample {

  def duplicate(value: Array[Byte]): Boolean = ???
  def ignore(value: Array[Byte]): Boolean = ???

  def main(args: Array[String]): Unit = {
    val done =
      // #oneToConditional
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .map(msg => {
          val out: Envelope[String, Array[Byte], CommittableOffset] =
            if (duplicate(msg.record.value))
              MultiMessage(
                immutable.Seq(
                  new ProducerRecord("topic2", msg.record.key, msg.record.value),
                  new ProducerRecord("topic3", msg.record.key, msg.record.value)
                ),
                msg.committableOffset
              )
            else if (ignore(msg.record.value))
              PassThroughMessage(msg.committableOffset)
            else
              Message(
                new ProducerRecord("topic2", msg.record.key, msg.record.value),
                msg.committableOffset
              )
          out
        })
        .via(Producer.flow2(producerSettings))
        .map(_.passThrough)
        .batch(max = 20, CommittableOffsetBatch(_))(_.updated(_))
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #oneToConditional

    terminateWhenDone(done)
  }
}
