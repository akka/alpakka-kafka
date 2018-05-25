package sample.scaladsl
// #oneToMany
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.ProducerRecord

// #oneToMany

// Connect a Consumer to Producer, mapping messages one-to-many, and commit in batches
object AtLeastOnceOneToManyExample extends ConsumerExample {

  def main(args: Array[String]): Unit = {
    val done =
      // #oneToMany
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapConcat(msg =>
          List(
            ProducerMessage.Message(
              new ProducerRecord[String, Array[Byte]]("topic2", msg.record.key, msg.record.value),
              None
            ),
            ProducerMessage.Message(
              new ProducerRecord[String, Array[Byte]]("topic2", msg.record.key, msg.record.value),
              Some(msg.committableOffset)
            )
          ))
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        .collect{ case Some(offset) => offset }
        .batch(max = 20, CommittableOffsetBatch(_))(_.updated(_))
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #oneToMany

    terminateWhenDone(done)
  }
}
