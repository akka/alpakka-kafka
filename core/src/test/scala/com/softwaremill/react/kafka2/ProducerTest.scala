package com.softwaremill.react.kafka2

import java.util.concurrent.{CompletableFuture, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializerSettings, ActorMaterializer}
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
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

    val probe = Source
      .empty[Record]
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

  it should "emit confirmation in same order as inputs" in {
    val input = Vector(
      new ProducerRecord("test", "1", "1") -> new RecordMetadata(new TopicPartition("test", 1), 1L, 1L),
      new ProducerRecord("test", "2", "2") -> new RecordMetadata(new TopicPartition("test", 2), 2L, 2L),
      new ProducerRecord("test", "3", "3") -> new RecordMetadata(new TopicPartition("test", 3), 3L, 3L)
    )

    val client = {
      val client = mock[KafkaProducer[K, V]]
      val answers = input.toMap
      when(client.send(any[Record], any[Callback])).thenAnswer(new Answer[java.util.concurrent.Future[RecordMetadata]] {
        override def answer(invocation: InvocationOnMock) = {
          val metadata = answers(invocation.getArguments()(0).asInstanceOf[Record])
          invocation.getArguments()(1).asInstanceOf[Callback].onCompletion(metadata, null)
          CompletableFuture.completedFuture(metadata)
        }
      })
      client
    }

    val producer = Producer(() => client)

    val probe = Source
      .fromIterator(() => input.map(_._1).toIterator)
      .via(producer)
      .mapAsync(1)(identity)
      .runWith(TestSink.probe[Any])

    probe
      .request(10)
      .expectNextN(input)
      .expectComplete()

    verify(client, atLeastOnce()).send(any[Record], any[Callback])
    verify(client).flush()
    verify(client).close(any[Long], any[TimeUnit])
    verifyNoMoreInteractions(client)

    ()
  }
}

