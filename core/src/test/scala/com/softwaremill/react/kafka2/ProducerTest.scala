package com.softwaremill.react.kafka2

import java.util.concurrent.{CompletableFuture, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSource, TestSink}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * @author Alexey Romanchuk
 */
class ProducerTest(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with MockitoSugar {
  def this() = this(ActorSystem())

  implicit val m = ActorMaterializer(ActorMaterializerSettings(_system))
  implicit val ec = _system.dispatcher

  type K = String
  type V = String
  type Record = ProducerRecord[K, V]

  "Producer" should "not send messages when source is empty" in {
    val client = new ProducerMock[K, V](ProducerMock.handlers.fail)

    val probe = Source
      .empty[Record]
      .via(Producer(() => client.mock))
      .runWith(TestSink.probe[Any])

    probe
      .request(1)
      .expectComplete()

    client.verifySend(never())
    client.verifyClosed()
    client.verifyNoMoreInteractions()
    ()
  }

  it should "emit confirmation in same order as inputs" in {
    val input = Vector(
      new ProducerRecord("test", "1", "1") -> new RecordMetadata(new TopicPartition("test", 1), 1L, 1L),
      new ProducerRecord("test", "2", "2") -> new RecordMetadata(new TopicPartition("test", 2), 2L, 2L),
      new ProducerRecord("test", "3", "3") -> new RecordMetadata(new TopicPartition("test", 3), 3L, 3L)
    )

    val client = {
      val inputMap = input.toMap
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100 millis)(inputMap))
    }
    val probe = Source
      .fromIterator(() => input.map(_._1).toIterator)
      .via(Producer(() => client.mock))
      .mapAsync(1)(identity)
      .runWith(TestSink.probe[(Record, RecordMetadata)])

    probe
      .request(10)
      .expectNextN(input)
      .expectComplete()

    client.verifyClosed()
    client.verifySend(atLeastOnce())
    client.verifyNoMoreInteractions()
    ()
  }

  it should "wait for confirmed messages in case of source error" in {
    val input = Vector(
      new ProducerRecord("test", "1", "1") -> new RecordMetadata(new TopicPartition("test", 1), 1L, 1L),
      new ProducerRecord("test", "2", "2") -> new RecordMetadata(new TopicPartition("test", 2), 2L, 2L),
      new ProducerRecord("test", "3", "3") -> new RecordMetadata(new TopicPartition("test", 3), 3L, 3L)
    )

    val client = {
      val inputMap = input.toMap
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100 millis)(inputMap))
    }
    val (source, sink) = TestSource
      .probe[Record]
      .via(Producer(() => client.mock))
      .mapAsync(1)(identity)
      .toMat(TestSink.probe[(Record, RecordMetadata)])(Keep.both)
      .run()

    sink.request(100)
    input.map(_._1).foreach(source.sendNext)
    source.sendError(new Exception())
    sink
      .expectNextN(input)
      .expectError()

    client.verifyClosed()
    client.verifySend(atLeastOnce())
    client.verifyNoMoreInteractions()
  }
}

object ProducerMock {
  type Handler[K, V] = (ProducerRecord[K, V], Callback) => Future[RecordMetadata]
  object handlers {
    def fail[K, V]: Handler[K, V] = (_, _) => throw new Exception("Should not be called")
    def instantMap[K, V](f: ProducerRecord[K, V] => RecordMetadata): Handler[K, V] = {
      (record, _) => Future.successful(f(record))
    }
    def delayedMap[K, V](delay: FiniteDuration)(f: ProducerRecord[K, V] => RecordMetadata)(implicit as: ActorSystem): Handler[K, V] = {
      (record, _) =>
        implicit val ec = as.dispatcher
        val promise = Promise[RecordMetadata]()
        as.scheduler.scheduleOnce(delay) {
          promise.success(f(record))
          ()
        }
        promise.future
    }
  }
}

class ProducerMock[K, V](handler: ProducerMock.Handler[K, V])(implicit ec: ExecutionContext) {
  var closed = false
  val mock = {
    val result = Mockito.mock(classOf[KafkaProducer[K, V]])
    Mockito.when(result.send(any[ProducerRecord[K, V]], any[Callback])).thenAnswer(new Answer[java.util.concurrent.Future[RecordMetadata]] {
      override def answer(invocation: InvocationOnMock) = {
        val record = invocation.getArguments()(0).asInstanceOf[ProducerRecord[K, V]]
        val callback = invocation.getArguments()(1).asInstanceOf[Callback]
        handler(record, callback).onComplete {
          case Success(value) if !closed => callback.onCompletion(value, null)
          case Success(value) if closed => callback.onCompletion(null, new Exception("Kafka producer already closed"))
          case Failure(ex: Exception) => callback.onCompletion(null, ex)
          case Failure(ex) => ???
        }
        val result = new CompletableFuture[RecordMetadata]()
        result.completeExceptionally(new Exception("Not implemented yet"))
        result
      }
    })
    Mockito.when(result.close(any[Long], any[TimeUnit])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        closed = true
        ()
      }
    })
    result
  }

  def verifySend(mode: VerificationMode) = {
    Mockito.verify(mock, mode).send(any[ProducerRecord[K, V]], any[Callback])
  }

  def verifyClosed() = {
    Mockito.verify(mock).flush()
    Mockito.verify(mock).close(any[Long], any[TimeUnit])
  }

  def verifyNoMoreInteractions() = {
    Mockito.verifyNoMoreInteractions(mock)
  }
}

