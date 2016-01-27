package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Flow, Sink}
import akka.stream.testkit.scaladsl.TestSource
import kafka.producer.ReactiveKafkaProducer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.FlatSpec

class ReactiveKafkaSubscriberSpec extends FlatSpec {

  implicit val actorSystem = ActorSystem("test")
  implicit val materializer = ActorMaterializer()

  "ReactiveKafkaSubscriber" should "cancel upstream if error occurs while sending messages" in {
    val kafkaProducerProperties = ProducerProperties(
      bootstrapServers = "localhost:8082",
      topic = "uris",
      valueSerializer = new StringSerializer()
    ).setProperty("max.block.ms", "100")
    val kafkaSubscriber = new ReactiveKafka().publish(kafkaProducerProperties)

    val sink = Sink.fromSubscriber(kafkaSubscriber)

    val probe = TestSource.probe[String]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    probe
      .sendNext("dummy")
      .expectCancellation()
    ()
  }

}
