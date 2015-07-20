package examples

import kafka.serializer.{StringDecoder, StringEncoder}

/**
 * Code samples for the documentation.
 */
object examples {

  def simple(): Unit = {

    import com.softwaremill.react.kafka.ReactiveKafka
    import com.softwaremill.react.kafka.ProducerProps
    import com.softwaremill.react.kafka.ConsumerProps
    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.scaladsl.{Sink, Source}

    implicit val actorSystem = ActorSystem("ReactiveKafka")
    implicit val materializer = ActorMaterializer()

    val kafka = new ReactiveKafka()
    val publisher = kafka.consume(ConsumerProps("localhost:9092", "localhost:2181", "lowercaseStrings", "groupName", new StringDecoder()))
    val subscriber = kafka.publish(ProducerProps("localhost:9092", "uppercaseSettings", "groupName", new StringEncoder()))

    Source(publisher).map(_.toUpperCase).to(Sink(subscriber)).run()
  }
}
