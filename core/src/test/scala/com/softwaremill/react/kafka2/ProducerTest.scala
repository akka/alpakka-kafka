package com.softwaremill.react.kafka2

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializerSettings, ActorMaterializer}
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpecLike, Matchers}

/**
 * @author Alexey Romanchuk
 */
class ProducerTest(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with MockitoSugar {
  def this() = this(ActorSystem())

  private implicit val m = ActorMaterializer(ActorMaterializerSettings(_system))

  type K = String
  type V = String
  type Record = ProducerRecord[K, V]

  "Producer" should "not send messages when source is empty" in {
    val client = mock[KafkaProducer[K, V]]
    val producer = Producer(() => client)

    val probe = Source.empty[Record]
      .via(producer)
      .runWith(TestSink.probe[Any])

    probe
      .request(1)
      .expectComplete()

    verify(client, never()).send(any[Record], any[Callback])
    verify(client).flush()
    verify(client).close(any[Long], any[TimeUnit])
    verifyNoMoreInteractions(client)
    ()
  }
}

