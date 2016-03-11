/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, SourceShape}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Await
import com.softwaremill.react.kafka2._
import scala.concurrent.duration._

// Reads from topic "dummy", processes messages and commits offset into kafka after processing.
// This provides at-least-once delivery guarantee. Also, shows how to perform graceful shutdown.
//
// Usage:
//    sbt core/test:run
//
object DummyConsumer extends App with LazyLogging {
  implicit val as = ActorSystem()
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(as)
      .withAutoFusing(false)
      .withInputBuffer(16, 16)
  )

  val provider = ConsumerProvider(
    "localhost:9092",
    new ByteArrayDeserializer,
    new StringDeserializer
  )
    .setup(TopicSubscription("dummy"))
    .groupId("c5")
    .autoCommit(false)
    .prop("auto.offset.reset", "earliest")

  val graph = GraphDSL.create(Consumer[Array[Byte], String](provider)) { implicit b => kafka =>
    import GraphDSL.Implicits._
    type In = ConsumerRecord[Array[Byte], String]
    val dummyProcessor = Flow[In].map{ x => Thread.sleep(1000); x }

    kafka.messages ~> dummyProcessor ~> Consumer.record2commit ~> kafka.commit
    SourceShape(kafka.confirmation)
  }

  val control =
    Source.fromGraph(graph)
      .mapAsync(8)(identity)
      .to(shutdownAsOnComplete)
      .run()

  sys.addShutdownHook {
    control.stop()

    println("Waiting for stop!")
    Await.result(as.whenTerminated, 30.seconds)
    println("AS stopped!")
  }
}
