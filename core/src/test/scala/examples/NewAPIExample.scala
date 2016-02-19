package examples

import akka.stream.ClosedShape
import akka.stream.scaladsl.{Flow, Sink, GraphDSL}
import com.softwaremill.react.kafka2.{Consumer, ConsumerProvider, TopicSubscription, ProducerProvider}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer, ByteArraySerializer}

/*
* Examples for https://github.com/softwaremill/reactive-kafka/blob/master/docs/NewAPI.md
*/
object NewAPIExample {

  //--- Graph ---
  {
    /* tbd. What's the 'provider' in the sample?
    //
    val graph = GraphDSL.create(Consumer[Array[Byte], String](provider)) { implicit b => kafka =>
      import GraphDSL.Implicits._
      type In = ConsumerRecord[Array[Byte], String]
      val processing = Flow[In].map{ x => ??? /* your logic here */ ; x }
      val shutdownHandler: Sink[Any, Unit] = ??? // your logic here

      kafka.messages ~> processing ~> Consumer.record2commit ~> kafka.commit
      kafka.confirmation ~> shutdownHandler
      ClosedShape
    }
    */
  }

  //--- Consumer provider ---
  {
    val consumerProvider =
      ConsumerProvider("localhost:9092", new ByteArrayDeserializer, new StringDeserializer)
        .setup(TopicSubscription("someTopic"))
        .groupId("myGroup")
        .autoCommit(false)
        .prop("auto.offset.reset", "earliest")

    val consumer = consumerProvider.apply()
  }

  //--- Producer provider ---
  {
    val producerProvider =
      ProducerProvider[Array[Byte], String]("localhost:9092", new ByteArraySerializer, new StringSerializer)
        .props("some.props" -> "value")

    val producer = producerProvider.apply()
  }

}
