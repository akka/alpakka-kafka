package com.softwaremill.react.kafka2

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

object DummyProducer extends App {
  implicit val as = ActorSystem()
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(as)
      .withAutoFusing(true)
      .withInputBuffer(1024, 1024)
  )
  val producer = ProducerProvider[Array[Byte], String](
    "localhost:9092",
    new ByteArraySerializer(),
    new StringSerializer()
  )

  Source
    .fromIterator(() => (1 to 10000).iterator)
    .map(_.toString)
    .via(Producer.value2record("dummy"))
    .via(Producer(producer))
    .mapAsync(1)(identity)
    .to(Streams.shutdownAsOnComplete)
    .run()
}
