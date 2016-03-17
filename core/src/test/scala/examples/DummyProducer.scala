/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import com.softwaremill.react.kafka2._

// Publishes 1 to 10000 (as strings) into Kafka and waits for publishing confirmation.
// Also provides an example of correct shutdown.
//
// Usage:
//    sbt core/test:run
//
object DummyProducer extends App {
  implicit val as = ActorSystem()
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(as)
      .withAutoFusing(true)
      .withInputBuffer(1024, 1024)
  )

  val provider = ProducerProvider[Array[Byte], String](
    "localhost:9092",
    new ByteArraySerializer(),
    new StringSerializer()
  )

  Source
    .fromIterator(() => (1 to 10000).iterator)
    .map(_.toString)
    .via(Producer.value2record("dummy"))
    .via(Producer(provider))
    .mapAsync(1)(identity)
    .to(shutdownAsOnComplete)
    .run()
}
